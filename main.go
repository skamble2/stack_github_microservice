package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"

	"github.com/lib/pq"

	_ "github.com/lib/pq"

	"github.com/google/go-github/github"
	"golang.org/x/oauth2"
)

// Define a struct to unmarshal the StackOverflow data
type StackOverflowPost struct {
	QuestionID    int                   `json:"question_id"`
	QuestionTitle string                `json:"title"`
	QuestionBody  string                `json:"body"`
	Answers       []StackOverflowAnswer `json:"answers"`
}

type StackOverflowAnswer struct {
	AnswerID   int    `json:"answer_id"`
	AnswerBody string `json:"body"`
}

// Define a struct for GithubPosts
type GithubPost struct {
	Type    string // "Question" or "Answer"
	Content string // Body of the post
}

// Function to insert StackOverflow questions and their answers into the database
func InsertStackOverflowData(db *sql.DB, posts []StackOverflowPost, tagName string) error {
	// Create table for this tag
	err := CreateStackOverflowTable(db, tagName)
	if err != nil {
		return fmt.Errorf("create table: %v", err)
	}

	questionInsertQuery := fmt.Sprintf(`INSERT INTO so_%s_questions (question_id, title, body, link) VALUES ($1, $2, $3, $4) ON CONFLICT (question_id) DO NOTHING;`, tagName)
	answerInsertQuery := fmt.Sprintf(`INSERT INTO so_%s_answers (answer_id, question_id, body) VALUES ($1, $2, $3) ON CONFLICT (answer_id) DO NOTHING;`, tagName)

	for _, post := range posts {
		// Insert the question
		_, err := db.Exec(questionInsertQuery, post.QuestionID, post.QuestionTitle, post.QuestionBody, "https://stackoverflow.com/q/"+strconv.Itoa(post.QuestionID))
		if err != nil {
			return fmt.Errorf("insert question: %v", err)
		}

		// Insert each answer for the current question
		for _, answer := range post.Answers {
			_, err := db.Exec(answerInsertQuery, answer.AnswerID, post.QuestionID, answer.AnswerBody)
			if err != nil {
				return fmt.Errorf("insert answer for question %d: %v", post.QuestionID, err)
			}
		}
	}
	return nil
}

// Function to create a table dynamically for GitHub data
func CreateGitHubTable(db *sql.DB, repoName string) error {
	createTableQuery := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS github_%s (
			id SERIAL PRIMARY KEY,
			title TEXT,
			body TEXT,
			labels TEXT[]
		);
	`, repoName)
	_, err := db.Exec(createTableQuery)
	return err
}

// Function to create a table dynamically for StackOverflow data
func CreateStackOverflowTable(db *sql.DB, tagName string) error {
	// Create questions table
	createQuestionsTableQuery := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS so_%s_questions (
			question_id INTEGER PRIMARY KEY,
			title TEXT,
			body TEXT,
			link TEXT
		);
	`, tagName)
	_, err := db.Exec(createQuestionsTableQuery)
	if err != nil {
		return err
	}

	// Create answers table
	createAnswersTableQuery := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS so_%s_answers (
			answer_id INTEGER PRIMARY KEY,
			question_id INTEGER,
			body TEXT,
			FOREIGN KEY (question_id) REFERENCES so_%s_questions(question_id)
		);
	`, tagName, tagName)
	_, err = db.Exec(createAnswersTableQuery)
	return err
}

// Function to insert GitHub data into the database
// Modified InsertGitHubData function
func InsertGitHubData(db *sql.DB, data []*GithubPost, repoName string) error {
	// Create table for this repo
	err := CreateGitHubTable(db, repoName)
	if err != nil {
		return fmt.Errorf("create table: %v", err)
	}

	for _, post := range data {
		_, err := db.Exec(fmt.Sprintf(`INSERT INTO github_%s (title, body, labels) VALUES ($1, $2, $3);`, repoName),
			post.Content, post.Content, pq.Array([]string{post.Type}))
		if err != nil {
			return err
		}
	}
	return nil
}

func getStackoverflowDBConnection() (*sql.DB, error) {
	// Replace with your StackOverflow database connection details
	connStr := "user=postgres dbname=StackoverflowDB password=root host=34.121.167.178 sslmode=disable"
	return sql.Open("postgres", connStr)
}

func getGitHubDBConnection() (*sql.DB, error) {
	// Replace with your GitHub database connection details
	connStr := "user=postgres dbname=GitHubDB password=root host=34.121.167.178 sslmode=disable"
	return sql.Open("postgres", connStr)
}

// Function to fetch questions and answers from StackOverflow
func GetStackOverflowPosts(tag string) ([]StackOverflowPost, error) {
	// Endpoint for fetching questions with tag
	questionsURL := fmt.Sprintf("https://api.stackexchange.com/2.2/questions?order=desc&sort=activity&tagged=%s&site=stackoverflow&filter=withbody&pagesize=5", tag)

	// Make the HTTP request to StackOverflow API
	questionResp, err := http.Get(questionsURL)
	if err != nil {
		return nil, err
	}
	defer questionResp.Body.Close()

	// Read and unmarshal the response body
	questionBody, err := ioutil.ReadAll(questionResp.Body)
	if err != nil {
		return nil, err
	}

	var questionsData struct {
		Items []struct {
			QuestionID int    `json:"question_id"`
			Title      string `json:"title"`
			Body       string `json:"body"`
		} `json:"items"`
	}

	err = json.Unmarshal(questionBody, &questionsData)
	if err != nil {
		return nil, err
	}

	// Prepare the slice of StackOverflowPost to hold questions and answers
	posts := make([]StackOverflowPost, 0, len(questionsData.Items))

	// Iterate over fetched questions to get answers
	for _, q := range questionsData.Items {
		// Endpoint for fetching answers for a specific question
		answersURL := fmt.Sprintf("https://api.stackexchange.com/2.2/questions/%d/answers?order=desc&sort=activity&site=stackoverflow&filter=withbody&pagesize=5", q.QuestionID)

		// Make the HTTP request to StackOverflow API for answers
		answerResp, err := http.Get(answersURL)
		if err != nil {
			return nil, err
		}
		defer answerResp.Body.Close()

		// Read and unmarshal the response body for answers
		answerBody, err := ioutil.ReadAll(answerResp.Body)
		if err != nil {
			return nil, err
		}

		var answersData struct {
			Items []StackOverflowAnswer `json:"items"`
		}

		err = json.Unmarshal(answerBody, &answersData)
		if err != nil {
			return nil, err
		}

		// Append the question and its answers to the posts slice
		posts = append(posts, StackOverflowPost{
			QuestionID:    q.QuestionID,
			QuestionTitle: q.Title,
			QuestionBody:  q.Body,
			Answers:       answersData.Items,
		})
	}

	return posts, nil
}

// Function to fetch questions (issues) and answers (comments) from a GitHub repository
func GetGitHubData(owner, repo string, accessToken string) ([]*GithubPost, error) {
	ctx := context.Background()
	ts := oauth2.StaticTokenSource(
		&oauth2.Token{AccessToken: accessToken},
	)
	tc := oauth2.NewClient(ctx, ts)
	client := github.NewClient(tc)

	// Fetching only open issues for the repository
	issues, _, err := client.Issues.ListByRepo(ctx, owner, repo, &github.IssueListByRepoOptions{State: "open"})
	if err != nil {
		return nil, err
	}

	var posts []*GithubPost

	// Iterate through the issues to fetch comments (answers)
	for _, issue := range issues {
		comments, _, err := client.Issues.ListComments(ctx, owner, repo, issue.GetNumber(), nil)
		if err != nil {
			return nil, err
		}

		// Create a GithubPost for the question (issue)
		question := &GithubPost{
			Type:    "Question",
			Content: issue.GetBody(),
		}
		posts = append(posts, question)

		// Create GithubPosts for the answers (comments)
		for _, comment := range comments {
			answer := &GithubPost{
				Type:    "Answer",
				Content: comment.GetBody(),
			}
			posts = append(posts, answer)
		}
	}

	return posts, nil
}

func main() {
	stackoverflowDB, err := getStackoverflowDBConnection()
	if err != nil {
		log.Fatalf("Error connecting to StackoverflowDB: %s\n", err)
	}
	defer stackoverflowDB.Close()

	githubDB, err := getGitHubDBConnection()
	if err != nil {
		log.Fatalf("Error connecting to GitHubDB: %s\n", err)
	}
	defer githubDB.Close()

	// Define a list of frameworks/libraries and their GitHub repositories
	frameworks := map[string][]string{
		"Prometheus": {"prometheus", "prometheus"},
		"Selenium":   {"SeleniumHQ", "selenium"},
		"OpenAI":     {"openai", "gym"},
		"Docker":     {"docker", "docker"},
		"Milvus":     {"milvus-io", "milvus"},
		"Go":         {"golang", "go"},
	}

	// Iterate over each framework/library
	for framework, repoInfo := range frameworks {
		// Fetch and insert StackOverflow data
		posts, err := GetStackOverflowPosts(framework)
		if err != nil {
			log.Fatalf("Error fetching StackOverflow posts for %s: %s\n", framework, err)
		}
		err = InsertStackOverflowData(stackoverflowDB, posts, framework) // Include the framework name
		if err != nil {
			log.Fatalf("Error inserting StackOverflow posts for %s into the database: %s\n", framework, err)
		}
		fmt.Printf("StackOverflow posts for %s inserted into the StackoverflowDB successfully.\n", framework)

		if len(repoInfo) == 2 {
			gitHubData, err := GetGitHubData(repoInfo[0], repoInfo[1], "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") // Replace with actual token
			if err != nil {
				log.Fatalf("Error fetching data from GitHub for %s: %s\n", framework, err)
			}
			err = InsertGitHubData(githubDB, gitHubData, repoInfo[1]) // Include the repository name
			if err != nil {
				log.Fatalf("Error inserting GitHub data for %s into the GithubDB: %s\n", framework, err)
			}
			fmt.Printf("GitHub data for %s inserted into the GithubDB successfully.\n", framework)
		} else {
			log.Printf("Invalid repository information for %s\n", framework)
		}
	}
}
