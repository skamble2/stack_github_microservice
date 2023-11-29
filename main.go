package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/lib/pq"

	_ "github.com/lib/pq"

	_ "github.com/GoogleCloudPlatform/cloudsql-proxy/proxy/dialers/postgres"
	"github.com/google/go-github/github"
	"github.com/joho/godotenv"
	"golang.org/x/oauth2"
)

// Define a struct to unmarshal the StackOverflow data
type StackOverflowPost struct {
	QuestionID    int                   `json:"question_id"`
	QuestionTitle string                `json:"title"`
	QuestionBody  string                `json:"body"`
	Answers       []StackOverflowAnswer `json:"answers"`
}

var (
	githubAPICalls = prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Name: "github_api_calls_per_second",
			Help: "Rate of API calls made to GitHub per second",
		},
		[]string{"endpoint"},
	)
	stackoverflowAPICalls = prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Name: "stackoverflow_api_calls_per_second",
			Help: "Rate of API calls made to StackOverflow per second",
		},
		[]string{"endpoint"},
	)
	dataCollectedPerSecond = prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Name: "data_collected_per_second",
			Help: "Amount of data collected per second",
		},
		[]string{"source"},
	)
	totalGithubAPICalls = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "total_github_api_calls",
			Help: "Total number of API calls made to GitHub",
		},
	)
	totalStackOverflowAPICalls = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "total_stackoverflow_api_calls",
			Help: "Total number of API calls made to StackOverflow",
		},
	)
	githubAPICalls2Days = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "github_api_calls_2_days_total",
			Help: "Total number of API calls made to GitHub in the past 2 days",
		},
	)
	githubAPICalls7Days = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "github_api_calls_7_days_total",
			Help: "Total number of API calls made to GitHub in the past 7 days",
		},
	)
	githubAPICalls45Days = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "github_api_calls_45_days_total",
			Help: "Total number of API calls made to GitHub in the past 45 days",
		},
	)

	// StackOverflow API call counters for different durations
	stackoverflowAPICalls2Days = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "stackoverflow_api_calls_2_days_total",
			Help: "Total number of API calls made to StackOverflow in the past 2 days",
		},
	)
	stackoverflowAPICalls7Days = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "stackoverflow_api_calls_7_days_total",
			Help: "Total number of API calls made to StackOverflow in the past 7 days",
		},
	)
	stackoverflowAPICalls45Days = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "stackoverflow_api_calls_45_days_total",
			Help: "Total number of API calls made to StackOverflow in the past 45 days",
		},
	)
)

func init() {
	prometheus.MustRegister(githubAPICalls, stackoverflowAPICalls, dataCollectedPerSecond)
	prometheus.MustRegister(totalGithubAPICalls, totalStackOverflowAPICalls)
	prometheus.MustRegister(
		githubAPICalls2Days, githubAPICalls7Days, githubAPICalls45Days,
		stackoverflowAPICalls2Days, stackoverflowAPICalls7Days, stackoverflowAPICalls45Days,
	)
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
	// Database connection settings
	connectionName := "/cloudsql/stack-github-microservice:us-central1:mypostgres" // Replace with the actual host or connection name
	dbUser := "postgres"
	dbPass := "root"
	dbName := "StackoverflowDB"

	dbURI := fmt.Sprintf("host=%s dbname=%s user=%s password=%s sslmode=disable",
		connectionName, dbName, dbUser, dbPass)

	// Initialize the SQL DB handle
	log.Println("Initializing Stackoverflow database connection")
	db, err := sql.Open("postgres", dbURI) // Changed from cloudsqlpostgres to postgres
	if err != nil {
		log.Fatalf("Error on initializing Stackoverflow database connection: %s", err.Error())
	}

	// Test the database connection
	log.Println("Testing Stackoverflow database connection")
	err = db.Ping()
	if err != nil {
		log.Fatalf("Error on Stackoverflow database connection: %s", err.Error())
	}
	log.Println("Stackoverflow Database connection established")

	return db, err
}

func getGitHubDBConnection() (*sql.DB, error) {
	// Database connection settings
	connectionName := "/cloudsql/stack-github-microservice:us-central1:mypostgres" // Replace with the actual host or connection name
	dbUser := "postgres"
	dbPass := "root"
	dbName := "GitHubDB" // Change to the actual GitHub database name

	dbURI := fmt.Sprintf("host=%s dbname=%s user=%s password=%s sslmode=disable",
		connectionName, dbName, dbUser, dbPass)

	// Initialize the SQL DB handle
	log.Println("Initializing GitHub database connection")
	db, err := sql.Open("postgres", dbURI) // Use "postgres" as the driver name
	if err != nil {
		log.Fatalf("Error on initializing GitHub database connection: %s", err.Error())
	}

	// Test the database connection
	log.Println("Testing GitHub database connection")
	err = db.Ping()
	if err != nil {
		log.Fatalf("Error on GitHub database connection: %s", err.Error())
	}
	log.Println("GitHub Database connection established")

	return db, err
}

// Function to fetch questions and answers from StackOverflow
func GetStackOverflowPosts(tag string, fromDate time.Time) ([]StackOverflowPost, error) {
	start := time.Now()
	// Endpoint for fetching questions with tag
	questionsURL := fmt.Sprintf("https://api.stackexchange.com/2.2/questions?order=desc&sort=activity&tagged=%s&site=stackoverflow&filter=withbody&pagesize=5&fromdate=%d", tag, fromDate.Unix())
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
	duration := time.Since(start).Seconds()
	if duration == 0 {
		duration = 1 // Avoid division by zero
	}
	rate := 1 / duration // Calculate the rate
	stackoverflowAPICalls.WithLabelValues("stackoverflow_endpoint").Observe(rate)
	totalStackOverflowAPICalls.Inc()

	return posts, nil
}

// Function to fetch questions (issues) and answers (comments) from a GitHub repository
func GetGitHubData(owner, repo string, accessToken string, fromDate time.Time) ([]*GithubPost, error) {
	start := time.Now()
	ctx := context.Background()
	ts := oauth2.StaticTokenSource(
		&oauth2.Token{AccessToken: accessToken},
	)
	tc := oauth2.NewClient(ctx, ts)
	client := github.NewClient(tc)

	issues, _, err := client.Issues.ListByRepo(ctx, owner, repo, &github.IssueListByRepoOptions{State: "open", Since: fromDate})
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

	duration := time.Since(start).Seconds()
	if duration == 0 {
		duration = 1 // Avoid division by zero
	}
	rate := 1 / duration // Calculate the rate
	githubAPICalls.WithLabelValues("github_endpoint").Observe(rate)
	totalGithubAPICalls.Inc()

	return posts, nil
}

func RunExperiment(duration time.Duration, stackoverflowDB *sql.DB, githubDB *sql.DB) {
	fromDate := time.Now().Add(-duration)

	err := godotenv.Load("config.env")
	if err != nil {
		log.Fatalf("Error loading .env file: %s", err)
	}

	githubToken := os.Getenv("GITHUB_TOKEN")
	if githubToken == "" {
		log.Fatal("GitHub token not found in environment variables")
	}

	// Define a list of frameworks/libraries and their GitHub repositories
	frameworks := map[string][]string{
		"Prometheus": {"prometheus", "prometheus"},
		"Selenium":   {"SeleniumHQ", "selenium"},
		"OpenAI":     {"openai", "gym"},
		"Docker":     {"docker", "docker"},
		"Milvus":     {"milvus-io", "milvus"},
		"Go":         {"golang", "go"},
	}

	var totalGithubCalls, totalStackOverflowCalls int

	// Iterate over each framework/library
	for framework, repoInfo := range frameworks {
		// Fetch and insert StackOverflow data
		posts, err := GetStackOverflowPosts(framework, fromDate)
		if err != nil {
			log.Fatalf("Error fetching StackOverflow posts for %s: %s\n", framework, err)
		}
		err = InsertStackOverflowData(stackoverflowDB, posts, framework)
		if err != nil {
			log.Fatalf("Error inserting StackOverflow posts for %s into the database: %s\n", framework, err)
		}
		totalStackOverflowCalls += len(posts) // Count the number of StackOverflow API calls

		// Fetch and insert GitHub data
		if len(repoInfo) == 2 {
			gitHubData, err := GetGitHubData(repoInfo[0], repoInfo[1], "githubToken", fromDate)
			if err != nil {
				log.Fatalf("Error fetching data from GitHub for %s: %s\n", framework, err)
			}
			err = InsertGitHubData(githubDB, gitHubData, repoInfo[1])
			if err != nil {
				log.Fatalf("Error inserting GitHub data for %s into the GithubDB: %s\n", framework, err)
			}
			totalGithubCalls += len(gitHubData) // Count the number of GitHub API calls
		} else {
			log.Printf("Invalid repository information for %s\n", framework)
		}
	}

	// Increment the appropriate counter based on the duration
	switch duration {
	case 2 * 24 * time.Hour:
		githubAPICalls2Days.Add(float64(totalGithubCalls))
		stackoverflowAPICalls2Days.Add(float64(totalStackOverflowCalls))
	case 7 * 24 * time.Hour:
		githubAPICalls7Days.Add(float64(totalGithubCalls))
		stackoverflowAPICalls7Days.Add(float64(totalStackOverflowCalls))
	case 45 * 24 * time.Hour:
		githubAPICalls45Days.Add(float64(totalGithubCalls))
		stackoverflowAPICalls45Days.Add(float64(totalStackOverflowCalls))
	}
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

	http.Handle("/metrics", promhttp.Handler())
	go http.ListenAndServe(":8080", nil)

	// Run experiments for different durations
	RunExperiment(2*24*time.Hour, stackoverflowDB, githubDB)  // For past 2 days
	RunExperiment(7*24*time.Hour, stackoverflowDB, githubDB)  // For past 7 days
	RunExperiment(45*24*time.Hour, stackoverflowDB, githubDB) // For past 45 days

	time.Sleep(24 * time.Hour)
}
