package main

import (
	"database/sql"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
)

// Database connection
var db *sql.DB

// Model structs
type User struct {
	ID        int       `json:"id" db:"id"`
	Name      string    `json:"name" db:"name"`
	Email     string    `json:"email" db:"email"`
	Age       int       `json:"age" db:"age"`
	CreatedAt time.Time `json:"created_at" db:"created_at"`
}

type Customer struct {
	ID          int    `json:"id" db:"id"`
	UserID      int    `json:"user_id" db:"user_id"`
	Company     string `json:"company" db:"company"`
	Country     string `json:"country" db:"country"`
	CreditLimit int    `json:"credit_limit" db:"credit_limit"`
}

type Product struct {
	ID       int    `json:"id" db:"id"`
	Name     string `json:"name" db:"name"`
	Category string `json:"category" db:"category"`
	Price    int    `json:"price" db:"price"`
	Stock    int    `json:"stock" db:"stock"`
}

type Order struct {
	ID          int       `json:"id" db:"id"`
	CustomerID  int       `json:"customer_id" db:"customer_id"`
	OrderDate   time.Time `json:"order_date" db:"order_date"`
	TotalAmount int       `json:"total_amount" db:"total_amount"`
	Status      string    `json:"status" db:"status"`
}

type OrderItem struct {
	ID        int `json:"id" db:"id"`
	OrderID   int `json:"order_id" db:"order_id"`
	ProductID int `json:"product_id" db:"product_id"`
	Quantity  int `json:"quantity" db:"quantity"`
	Price     int `json:"price" db:"price"`
}

type Department struct {
	ID        int    `json:"id" db:"id"`
	Name      string `json:"name" db:"name"`
	Budget    int    `json:"budget" db:"budget"`
	HeadCount int    `json:"head_count" db:"head_count"`
}

type Employee struct {
	ID           int       `json:"id" db:"id"`
	Name         string    `json:"name" db:"name"`
	DepartmentID int       `json:"department_id" db:"department_id"`
	Salary       int       `json:"salary" db:"salary"`
	HireDate     time.Time `json:"hire_date" db:"hire_date"`
}

type Transaction struct {
	ID        int       `json:"id" db:"id"`
	UserID    int       `json:"user_id" db:"user_id"`
	Amount    int       `json:"amount" db:"amount"`
	Type      string    `json:"type" db:"type"`
	Timestamp time.Time `json:"timestamp" db:"timestamp"`
}

type Inventory struct {
	ID          int       `json:"id" db:"id"`
	ProductID   int       `json:"product_id" db:"product_id"`
	Warehouse   string    `json:"warehouse" db:"warehouse"`
	Quantity    int       `json:"quantity" db:"quantity"`
	LastUpdated time.Time `json:"last_updated" db:"last_updated"`
}

type Review struct {
	ID        int       `json:"id" db:"id"`
	ProductID int       `json:"product_id" db:"product_id"`
	UserID    int       `json:"user_id" db:"user_id"`
	Rating    int       `json:"rating" db:"rating"`
	CreatedAt time.Time `json:"created_at" db:"created_at"`
}

// Response structures
type APIResponse struct {
	Success bool        `json:"success"`
	Data    interface{} `json:"data,omitempty"`
	Error   string      `json:"error,omitempty"`
	Count   int         `json:"count,omitempty"`
}

type DatabaseStats struct {
	Tables      []TableStats `json:"tables"`
	TotalRows   int          `json:"total_rows"`
	QueryCount  int          `json:"query_count"`
	ConnectedDB string       `json:"connected_db"`
}

type TableStats struct {
	Name     string `json:"name"`
	RowCount int    `json:"row_count"`
}

func main() {
	rand.Seed(time.Now().UnixNano())

	// Load environment variables from .env file
	if err := godotenv.Load(); err != nil {
		log.Println("No .env file found, using default configuration")
	}

	// Initialize database connection
	var err error

	// Get DATABASE_URL from environment or use default
	connStr := os.Getenv("DATABASE_URL")
	if connStr == "" {
		connStr = "host=localhost port=5435 user=fastpostgres dbname=fastpostgres sslmode=disable"
		log.Printf("No DATABASE_URL found, using default: %s", connStr)
	} else {
		log.Printf("Using DATABASE_URL from environment: %s", connStr)
	}

	db, err = sql.Open("postgres", connStr)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close()

	// Test connection
	if err = db.Ping(); err != nil {
		log.Printf("Database not available: %v", err)
		log.Printf("Make sure FastPostgres server is running: ./fastpostgres server")
		log.Fatalf("Cannot connect to database")
	}

	log.Println("✓ Connected to FastPostgres database successfully!")

	// Initialize database schema and seed data
	initializeDatabase()

	// Setup Gin router
	r := setupRoutes()

	log.Println("🚀 Starting FastPostgres REST API Server")
	log.Println("📍 Server URL: http://localhost:8080")
	log.Println("📖 API Documentation:")
	log.Println("   GET  /api/health              - Health check")
	log.Println("   GET  /api/stats               - Database statistics")
	log.Println("   GET  /api/users               - List users")
	log.Println("   GET  /api/users/:id           - Get user by ID")
	log.Println("   GET  /api/products            - List products")
	log.Println("   GET  /api/analytics/summary   - Analytics summary")
	log.Println("   GET  /api/complex/user-orders - User orders with JOIN")
	log.Println("   ... and more endpoints")

	if err := r.Run(":8080"); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}

func setupRoutes() *gin.Engine {
	r := gin.Default()

	// Add CORS middleware
	r.Use(func(c *gin.Context) {
		c.Header("Access-Control-Allow-Origin", "*")
		c.Header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		c.Header("Access-Control-Allow-Headers", "Content-Type, Authorization")

		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(204)
			return
		}

		c.Next()
	})

	// API routes
	api := r.Group("/api")
	{
		// Basic endpoints
		api.GET("/health", healthCheck)
		api.GET("/stats", getDatabaseStats)

		// Table endpoints
		api.GET("/users", getUsers)
		api.GET("/users/:id", getUserByID)
		api.GET("/customers", getCustomers)
		api.GET("/products", getProducts)
		api.GET("/products/category/:category", getProductsByCategory)
		api.GET("/orders", getOrders)
		api.GET("/orders/:id", getOrderByID)
		api.GET("/employees", getEmployees)
		api.GET("/transactions", getTransactions)
		api.GET("/inventory", getInventory)
		api.GET("/reviews", getReviews)

		// Filtered queries
		api.GET("/users/age/:min", getUsersByMinAge)
		api.GET("/products/price/:min/:max", getProductsByPriceRange)
		api.GET("/transactions/amount/:min", getHighValueTransactions)
		api.GET("/reviews/rating/:min", getHighRatedReviews)

		// Aggregation endpoints
		api.GET("/analytics/summary", getAnalyticsSummary)
		api.GET("/analytics/users/age-groups", getUserAgeGroups)
		api.GET("/analytics/products/categories", getProductCategories)
		api.GET("/analytics/orders/monthly", getMonthlyOrders)

		// Complex queries with JOINs
		api.GET("/complex/user-orders", getUserOrders)
		api.GET("/complex/product-reviews", getProductReviews)
		api.GET("/complex/employee-departments", getEmployeeDepartments)
		api.GET("/complex/inventory-products", getInventoryProducts)
		api.GET("/complex/customer-orders/:id", getCustomerOrderDetails)

		// Performance testing endpoints
		api.GET("/perf/concurrent/:count", performanceConcurrentQueries)
		api.GET("/perf/bulk-insert/:count", performanceBulkInsert)
	}

	return r
}

func initializeDatabase() {
	log.Println("🔨 Initializing database schema...")

	// Create tables
	createTables()

	// Check if data exists
	var count int
	db.QueryRow("SELECT COUNT(*) FROM users").Scan(&count)

	if count == 0 {
		log.Println("📊 Seeding database with test data...")
		seedDatabase()
		log.Println("✓ Database seeding completed!")
	} else {
		log.Printf("✓ Database already contains %d users (skipping seed)", count)
	}
}

func createTables() {
	tables := []string{
		`CREATE TABLE IF NOT EXISTS users (
			id SERIAL PRIMARY KEY,
			name VARCHAR(100),
			email VARCHAR(100),
			age INTEGER,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)`,
		`CREATE TABLE IF NOT EXISTS customers (
			id SERIAL PRIMARY KEY,
			user_id INTEGER,
			company VARCHAR(100),
			country VARCHAR(50),
			credit_limit INTEGER
		)`,
		`CREATE TABLE IF NOT EXISTS products (
			id SERIAL PRIMARY KEY,
			name VARCHAR(100),
			category VARCHAR(50),
			price INTEGER,
			stock INTEGER
		)`,
		`CREATE TABLE IF NOT EXISTS orders (
			id SERIAL PRIMARY KEY,
			customer_id INTEGER,
			order_date TIMESTAMP,
			total_amount INTEGER,
			status VARCHAR(20)
		)`,
		`CREATE TABLE IF NOT EXISTS order_items (
			id SERIAL PRIMARY KEY,
			order_id INTEGER,
			product_id INTEGER,
			quantity INTEGER,
			price INTEGER
		)`,
		`CREATE TABLE IF NOT EXISTS departments (
			id SERIAL PRIMARY KEY,
			name VARCHAR(100),
			budget INTEGER,
			head_count INTEGER
		)`,
		`CREATE TABLE IF NOT EXISTS employees (
			id SERIAL PRIMARY KEY,
			name VARCHAR(100),
			department_id INTEGER,
			salary INTEGER,
			hire_date TIMESTAMP
		)`,
		`CREATE TABLE IF NOT EXISTS transactions (
			id SERIAL PRIMARY KEY,
			user_id INTEGER,
			amount INTEGER,
			type VARCHAR(20),
			timestamp TIMESTAMP
		)`,
		`CREATE TABLE IF NOT EXISTS inventory (
			id SERIAL PRIMARY KEY,
			product_id INTEGER,
			warehouse VARCHAR(50),
			quantity INTEGER,
			last_updated TIMESTAMP
		)`,
		`CREATE TABLE IF NOT EXISTS reviews (
			id SERIAL PRIMARY KEY,
			product_id INTEGER,
			user_id INTEGER,
			rating INTEGER,
			created_at TIMESTAMP
		)`,
	}

	for _, table := range tables {
		if _, err := db.Exec(table); err != nil {
			log.Printf("Error creating table: %v", err)
		}
	}
}

func seedDatabase() {
	countries := []string{"USA", "UK", "Canada", "Germany", "France", "Japan", "Australia", "Brazil"}
	categories := []string{"Electronics", "Clothing", "Food", "Books", "Toys", "Sports", "Home", "Beauty"}
	statuses := []string{"pending", "processing", "shipped", "delivered", "cancelled"}
	txTypes := []string{"debit", "credit", "refund", "fee"}
	warehouses := []string{"WH-EAST", "WH-WEST", "WH-NORTH", "WH-SOUTH", "WH-CENTRAL"}

	const ROWS_PER_TABLE = 1000

	log.Printf("  → Seeding %d users...", ROWS_PER_TABLE)
	for i := 0; i < ROWS_PER_TABLE; i++ {
		_, err := db.Exec(`INSERT INTO users (name, email, age, created_at) VALUES ($1, $2, $3, $4)`,
			fmt.Sprintf("User_%d", i+1),
			fmt.Sprintf("user%d@example.com", i+1),
			18+rand.Intn(60),
			time.Now().Add(-time.Duration(rand.Intn(365*24))*time.Hour))
		if err != nil {
			log.Printf("Error inserting user: %v", err)
		}
	}

	log.Printf("  → Seeding %d customers...", ROWS_PER_TABLE)
	for i := 0; i < ROWS_PER_TABLE; i++ {
		_, err := db.Exec(`INSERT INTO customers (user_id, company, country, credit_limit) VALUES ($1, $2, $3, $4)`,
			(i%ROWS_PER_TABLE)+1,
			fmt.Sprintf("Company_%d", i+1),
			countries[rand.Intn(len(countries))],
			1000+rand.Intn(50000))
		if err != nil {
			log.Printf("Error inserting customer: %v", err)
		}
	}

	log.Printf("  → Seeding %d products...", ROWS_PER_TABLE)
	for i := 0; i < ROWS_PER_TABLE; i++ {
		_, err := db.Exec(`INSERT INTO products (name, category, price, stock) VALUES ($1, $2, $3, $4)`,
			fmt.Sprintf("Product_%d", i+1),
			categories[rand.Intn(len(categories))],
			10+rand.Intn(1000),
			rand.Intn(500))
		if err != nil {
			log.Printf("Error inserting product: %v", err)
		}
	}

	log.Printf("  → Seeding %d orders...", ROWS_PER_TABLE)
	for i := 0; i < ROWS_PER_TABLE; i++ {
		_, err := db.Exec(`INSERT INTO orders (customer_id, order_date, total_amount, status) VALUES ($1, $2, $3, $4)`,
			(i%ROWS_PER_TABLE)+1,
			time.Now().Add(-time.Duration(rand.Intn(180*24))*time.Hour),
			20+rand.Intn(5000),
			statuses[rand.Intn(len(statuses))])
		if err != nil {
			log.Printf("Error inserting order: %v", err)
		}
	}

	log.Printf("  → Seeding %d order items...", ROWS_PER_TABLE)
	for i := 0; i < ROWS_PER_TABLE; i++ {
		_, err := db.Exec(`INSERT INTO order_items (order_id, product_id, quantity, price) VALUES ($1, $2, $3, $4)`,
			(i%ROWS_PER_TABLE)+1,
			rand.Intn(ROWS_PER_TABLE)+1,
			1+rand.Intn(10),
			10+rand.Intn(500))
		if err != nil {
			log.Printf("Error inserting order item: %v", err)
		}
	}

	log.Printf("  → Seeding %d departments...", ROWS_PER_TABLE)
	for i := 0; i < ROWS_PER_TABLE; i++ {
		_, err := db.Exec(`INSERT INTO departments (name, budget, head_count) VALUES ($1, $2, $3)`,
			fmt.Sprintf("Dept_%d", i+1),
			50000+rand.Intn(500000),
			5+rand.Intn(50))
		if err != nil {
			log.Printf("Error inserting department: %v", err)
		}
	}

	log.Printf("  → Seeding %d employees...", ROWS_PER_TABLE)
	for i := 0; i < ROWS_PER_TABLE; i++ {
		_, err := db.Exec(`INSERT INTO employees (name, department_id, salary, hire_date) VALUES ($1, $2, $3, $4)`,
			fmt.Sprintf("Employee_%d", i+1),
			(i%ROWS_PER_TABLE)+1,
			30000+rand.Intn(120000),
			time.Now().Add(-time.Duration(rand.Intn(10*365*24))*time.Hour))
		if err != nil {
			log.Printf("Error inserting employee: %v", err)
		}
	}

	log.Printf("  → Seeding %d transactions...", ROWS_PER_TABLE)
	for i := 0; i < ROWS_PER_TABLE; i++ {
		_, err := db.Exec(`INSERT INTO transactions (user_id, amount, type, timestamp) VALUES ($1, $2, $3, $4)`,
			(i%ROWS_PER_TABLE)+1,
			1+rand.Intn(10000),
			txTypes[rand.Intn(len(txTypes))],
			time.Now().Add(-time.Duration(rand.Intn(30*24))*time.Hour))
		if err != nil {
			log.Printf("Error inserting transaction: %v", err)
		}
	}

	log.Printf("  → Seeding %d inventory records...", ROWS_PER_TABLE)
	for i := 0; i < ROWS_PER_TABLE; i++ {
		_, err := db.Exec(`INSERT INTO inventory (product_id, warehouse, quantity, last_updated) VALUES ($1, $2, $3, $4)`,
			(i%ROWS_PER_TABLE)+1,
			warehouses[rand.Intn(len(warehouses))],
			rand.Intn(1000),
			time.Now().Add(-time.Duration(rand.Intn(7*24))*time.Hour))
		if err != nil {
			log.Printf("Error inserting inventory: %v", err)
		}
	}

	log.Printf("  → Seeding %d reviews...", ROWS_PER_TABLE)
	for i := 0; i < ROWS_PER_TABLE; i++ {
		_, err := db.Exec(`INSERT INTO reviews (product_id, user_id, rating, created_at) VALUES ($1, $2, $3, $4)`,
			rand.Intn(ROWS_PER_TABLE)+1,
			rand.Intn(ROWS_PER_TABLE)+1,
			1+rand.Intn(5),
			time.Now().Add(-time.Duration(rand.Intn(90*24))*time.Hour))
		if err != nil {
			log.Printf("Error inserting review: %v", err)
		}
	}
}

// API Handlers
func healthCheck(c *gin.Context) {
	c.JSON(http.StatusOK, APIResponse{
		Success: true,
		Data: gin.H{
			"status":    "healthy",
			"timestamp": time.Now(),
			"database":  "FastPostgres",
			"version":   "1.0.0",
		},
	})
}

func getDatabaseStats(c *gin.Context) {
	tables := []string{"users", "customers", "products", "orders", "order_items",
					 "departments", "employees", "transactions", "inventory", "reviews"}

	var stats DatabaseStats
	stats.Tables = make([]TableStats, len(tables))
	stats.ConnectedDB = "FastPostgres"

	for i, table := range tables {
		var count int
		err := db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", table)).Scan(&count)
		if err != nil {
			c.JSON(http.StatusInternalServerError, APIResponse{
				Success: false,
				Error:   err.Error(),
			})
			return
		}
		stats.Tables[i] = TableStats{Name: table, RowCount: count}
		stats.TotalRows += count
	}

	c.JSON(http.StatusOK, APIResponse{
		Success: true,
		Data:    stats,
	})
}

func getUsers(c *gin.Context) {
	limit := 100
	if l := c.Query("limit"); l != "" {
		if parsed, err := strconv.Atoi(l); err == nil {
			limit = parsed
		}
	}

	rows, err := db.Query("SELECT id, name, email, age, created_at FROM users LIMIT $1", limit)
	if err != nil {
		c.JSON(http.StatusInternalServerError, APIResponse{
			Success: false,
			Error:   err.Error(),
		})
		return
	}
	defer rows.Close()

	var users []User
	for rows.Next() {
		var user User
		err := rows.Scan(&user.ID, &user.Name, &user.Email, &user.Age, &user.CreatedAt)
		if err != nil {
			continue
		}
		users = append(users, user)
	}

	c.JSON(http.StatusOK, APIResponse{
		Success: true,
		Data:    users,
		Count:   len(users),
	})
}

func getUserByID(c *gin.Context) {
	id := c.Param("id")

	var user User
	err := db.QueryRow("SELECT id, name, email, age, created_at FROM users WHERE id = $1", id).
		Scan(&user.ID, &user.Name, &user.Email, &user.Age, &user.CreatedAt)

	if err == sql.ErrNoRows {
		c.JSON(http.StatusNotFound, APIResponse{
			Success: false,
			Error:   "User not found",
		})
		return
	}

	if err != nil {
		c.JSON(http.StatusInternalServerError, APIResponse{
			Success: false,
			Error:   err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, APIResponse{
		Success: true,
		Data:    user,
	})
}

func getProducts(c *gin.Context) {
	limit := 100
	if l := c.Query("limit"); l != "" {
		if parsed, err := strconv.Atoi(l); err == nil {
			limit = parsed
		}
	}

	rows, err := db.Query("SELECT id, name, category, price, stock FROM products LIMIT $1", limit)
	if err != nil {
		c.JSON(http.StatusInternalServerError, APIResponse{
			Success: false,
			Error:   err.Error(),
		})
		return
	}
	defer rows.Close()

	var products []Product
	for rows.Next() {
		var product Product
		err := rows.Scan(&product.ID, &product.Name, &product.Category, &product.Price, &product.Stock)
		if err != nil {
			continue
		}
		products = append(products, product)
	}

	c.JSON(http.StatusOK, APIResponse{
		Success: true,
		Data:    products,
		Count:   len(products),
	})
}

func getProductsByCategory(c *gin.Context) {
	category := c.Param("category")

	rows, err := db.Query("SELECT id, name, category, price, stock FROM products WHERE category = $1", category)
	if err != nil {
		c.JSON(http.StatusInternalServerError, APIResponse{
			Success: false,
			Error:   err.Error(),
		})
		return
	}
	defer rows.Close()

	var products []Product
	for rows.Next() {
		var product Product
		err := rows.Scan(&product.ID, &product.Name, &product.Category, &product.Price, &product.Stock)
		if err != nil {
			continue
		}
		products = append(products, product)
	}

	c.JSON(http.StatusOK, APIResponse{
		Success: true,
		Data:    products,
		Count:   len(products),
	})
}

func getUsersByMinAge(c *gin.Context) {
	minAge := c.Param("min")

	rows, err := db.Query("SELECT id, name, email, age, created_at FROM users WHERE age >= $1", minAge)
	if err != nil {
		c.JSON(http.StatusInternalServerError, APIResponse{
			Success: false,
			Error:   err.Error(),
		})
		return
	}
	defer rows.Close()

	var users []User
	for rows.Next() {
		var user User
		err := rows.Scan(&user.ID, &user.Name, &user.Email, &user.Age, &user.CreatedAt)
		if err != nil {
			continue
		}
		users = append(users, user)
	}

	c.JSON(http.StatusOK, APIResponse{
		Success: true,
		Data:    users,
		Count:   len(users),
	})
}

func getAnalyticsSummary(c *gin.Context) {
	summary := gin.H{}

	// Total counts
	tables := []string{"users", "products", "orders", "transactions", "reviews"}
	for _, table := range tables {
		var count int
		db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", table)).Scan(&count)
		summary[table+"_count"] = count
	}

	// Aggregations
	var avgAge float64
	db.QueryRow("SELECT AVG(age) FROM users").Scan(&avgAge)
	summary["avg_user_age"] = avgAge

	var totalRevenue int
	db.QueryRow("SELECT SUM(total_amount) FROM orders").Scan(&totalRevenue)
	summary["total_revenue"] = totalRevenue

	var avgRating float64
	db.QueryRow("SELECT AVG(rating) FROM reviews").Scan(&avgRating)
	summary["avg_rating"] = avgRating

	c.JSON(http.StatusOK, APIResponse{
		Success: true,
		Data:    summary,
	})
}

func getUserOrders(c *gin.Context) {
	query := `
		SELECT u.id, u.name, u.email, o.id, o.total_amount, o.status, o.order_date
		FROM users u
		LEFT JOIN customers c ON u.id = c.user_id
		LEFT JOIN orders o ON c.id = o.customer_id
		WHERE o.id IS NOT NULL
		LIMIT 50
	`

	rows, err := db.Query(query)
	if err != nil {
		c.JSON(http.StatusInternalServerError, APIResponse{
			Success: false,
			Error:   err.Error(),
		})
		return
	}
	defer rows.Close()

	var results []gin.H
	for rows.Next() {
		var userID, orderID, totalAmount int
		var userName, email, status string
		var orderDate time.Time

		err := rows.Scan(&userID, &userName, &email, &orderID, &totalAmount, &status, &orderDate)
		if err != nil {
			continue
		}

		results = append(results, gin.H{
			"user_id":      userID,
			"user_name":    userName,
			"user_email":   email,
			"order_id":     orderID,
			"total_amount": totalAmount,
			"status":       status,
			"order_date":   orderDate,
		})
	}

	c.JSON(http.StatusOK, APIResponse{
		Success: true,
		Data:    results,
		Count:   len(results),
	})
}

func performanceConcurrentQueries(c *gin.Context) {
	countStr := c.Param("count")
	count, err := strconv.Atoi(countStr)
	if err != nil || count <= 0 || count > 1000 {
		count = 100
	}

	start := time.Now()

	// Run concurrent queries
	results := make(chan int, count)
	for i := 0; i < count; i++ {
		go func() {
			var userCount int
			db.QueryRow("SELECT COUNT(*) FROM users").Scan(&userCount)
			results <- userCount
		}()
	}

	// Collect results
	var totalResults []int
	for i := 0; i < count; i++ {
		totalResults = append(totalResults, <-results)
	}

	duration := time.Since(start)

	c.JSON(http.StatusOK, APIResponse{
		Success: true,
		Data: gin.H{
			"concurrent_queries": count,
			"duration_ms":       duration.Milliseconds(),
			"queries_per_sec":   float64(count) / duration.Seconds(),
			"avg_result":        totalResults[0], // They should all be the same
		},
	})
}

// Additional endpoint implementations
func getCustomers(c *gin.Context) {
	limit := 100
	if l := c.Query("limit"); l != "" {
		if parsed, err := strconv.Atoi(l); err == nil {
			limit = parsed
		}
	}

	rows, err := db.Query("SELECT id, user_id, company, country, credit_limit FROM customers LIMIT $1", limit)
	if err != nil {
		c.JSON(http.StatusInternalServerError, APIResponse{Success: false, Error: err.Error()})
		return
	}
	defer rows.Close()

	var customers []Customer
	for rows.Next() {
		var customer Customer
		err := rows.Scan(&customer.ID, &customer.UserID, &customer.Company, &customer.Country, &customer.CreditLimit)
		if err != nil {
			continue
		}
		customers = append(customers, customer)
	}

	c.JSON(http.StatusOK, APIResponse{Success: true, Data: customers, Count: len(customers)})
}

func getOrders(c *gin.Context) {
	limit := 100
	if l := c.Query("limit"); l != "" {
		if parsed, err := strconv.Atoi(l); err == nil {
			limit = parsed
		}
	}

	rows, err := db.Query("SELECT id, customer_id, order_date, total_amount, status FROM orders LIMIT $1", limit)
	if err != nil {
		c.JSON(http.StatusInternalServerError, APIResponse{Success: false, Error: err.Error()})
		return
	}
	defer rows.Close()

	var orders []Order
	for rows.Next() {
		var order Order
		err := rows.Scan(&order.ID, &order.CustomerID, &order.OrderDate, &order.TotalAmount, &order.Status)
		if err != nil {
			continue
		}
		orders = append(orders, order)
	}

	c.JSON(http.StatusOK, APIResponse{Success: true, Data: orders, Count: len(orders)})
}

func getOrderByID(c *gin.Context) {
	id := c.Param("id")

	var order Order
	err := db.QueryRow("SELECT id, customer_id, order_date, total_amount, status FROM orders WHERE id = $1", id).
		Scan(&order.ID, &order.CustomerID, &order.OrderDate, &order.TotalAmount, &order.Status)

	if err == sql.ErrNoRows {
		c.JSON(http.StatusNotFound, APIResponse{Success: false, Error: "Order not found"})
		return
	}

	if err != nil {
		c.JSON(http.StatusInternalServerError, APIResponse{Success: false, Error: err.Error()})
		return
	}

	c.JSON(http.StatusOK, APIResponse{Success: true, Data: order})
}

func getEmployees(c *gin.Context) {
	limit := 100
	if l := c.Query("limit"); l != "" {
		if parsed, err := strconv.Atoi(l); err == nil {
			limit = parsed
		}
	}

	rows, err := db.Query("SELECT id, name, department_id, salary, hire_date FROM employees LIMIT $1", limit)
	if err != nil {
		c.JSON(http.StatusInternalServerError, APIResponse{Success: false, Error: err.Error()})
		return
	}
	defer rows.Close()

	var employees []Employee
	for rows.Next() {
		var employee Employee
		err := rows.Scan(&employee.ID, &employee.Name, &employee.DepartmentID, &employee.Salary, &employee.HireDate)
		if err != nil {
			continue
		}
		employees = append(employees, employee)
	}

	c.JSON(http.StatusOK, APIResponse{Success: true, Data: employees, Count: len(employees)})
}

func getTransactions(c *gin.Context) {
	limit := 100
	if l := c.Query("limit"); l != "" {
		if parsed, err := strconv.Atoi(l); err == nil {
			limit = parsed
		}
	}

	rows, err := db.Query("SELECT id, user_id, amount, type, timestamp FROM transactions LIMIT $1", limit)
	if err != nil {
		c.JSON(http.StatusInternalServerError, APIResponse{Success: false, Error: err.Error()})
		return
	}
	defer rows.Close()

	var transactions []Transaction
	for rows.Next() {
		var transaction Transaction
		err := rows.Scan(&transaction.ID, &transaction.UserID, &transaction.Amount, &transaction.Type, &transaction.Timestamp)
		if err != nil {
			continue
		}
		transactions = append(transactions, transaction)
	}

	c.JSON(http.StatusOK, APIResponse{Success: true, Data: transactions, Count: len(transactions)})
}

func getInventory(c *gin.Context) {
	limit := 100
	if l := c.Query("limit"); l != "" {
		if parsed, err := strconv.Atoi(l); err == nil {
			limit = parsed
		}
	}

	rows, err := db.Query("SELECT id, product_id, warehouse, quantity, last_updated FROM inventory LIMIT $1", limit)
	if err != nil {
		c.JSON(http.StatusInternalServerError, APIResponse{Success: false, Error: err.Error()})
		return
	}
	defer rows.Close()

	var inventory []Inventory
	for rows.Next() {
		var item Inventory
		err := rows.Scan(&item.ID, &item.ProductID, &item.Warehouse, &item.Quantity, &item.LastUpdated)
		if err != nil {
			continue
		}
		inventory = append(inventory, item)
	}

	c.JSON(http.StatusOK, APIResponse{Success: true, Data: inventory, Count: len(inventory)})
}

func getReviews(c *gin.Context) {
	limit := 100
	if l := c.Query("limit"); l != "" {
		if parsed, err := strconv.Atoi(l); err == nil {
			limit = parsed
		}
	}

	rows, err := db.Query("SELECT id, product_id, user_id, rating, created_at FROM reviews LIMIT $1", limit)
	if err != nil {
		c.JSON(http.StatusInternalServerError, APIResponse{Success: false, Error: err.Error()})
		return
	}
	defer rows.Close()

	var reviews []Review
	for rows.Next() {
		var review Review
		err := rows.Scan(&review.ID, &review.ProductID, &review.UserID, &review.Rating, &review.CreatedAt)
		if err != nil {
			continue
		}
		reviews = append(reviews, review)
	}

	c.JSON(http.StatusOK, APIResponse{Success: true, Data: reviews, Count: len(reviews)})
}

// Stub implementations for remaining endpoints
func getProductsByPriceRange(c *gin.Context) {
	c.JSON(http.StatusOK, APIResponse{Success: true, Data: "Price range query - implementation needed"})
}

func getHighValueTransactions(c *gin.Context) {
	c.JSON(http.StatusOK, APIResponse{Success: true, Data: "High value transactions - implementation needed"})
}

func getHighRatedReviews(c *gin.Context) {
	c.JSON(http.StatusOK, APIResponse{Success: true, Data: "High rated reviews - implementation needed"})
}

func getUserAgeGroups(c *gin.Context) {
	c.JSON(http.StatusOK, APIResponse{Success: true, Data: "User age groups - implementation needed"})
}

func getProductCategories(c *gin.Context) {
	c.JSON(http.StatusOK, APIResponse{Success: true, Data: "Product categories - implementation needed"})
}

func getMonthlyOrders(c *gin.Context) {
	c.JSON(http.StatusOK, APIResponse{Success: true, Data: "Monthly orders - implementation needed"})
}

func getProductReviews(c *gin.Context) {
	c.JSON(http.StatusOK, APIResponse{Success: true, Data: "Product reviews JOIN - implementation needed"})
}

func getEmployeeDepartments(c *gin.Context) {
	c.JSON(http.StatusOK, APIResponse{Success: true, Data: "Employee departments JOIN - implementation needed"})
}

func getInventoryProducts(c *gin.Context) {
	c.JSON(http.StatusOK, APIResponse{Success: true, Data: "Inventory products JOIN - implementation needed"})
}

func getCustomerOrderDetails(c *gin.Context) {
	c.JSON(http.StatusOK, APIResponse{Success: true, Data: "Customer order details - implementation needed"})
}

func performanceBulkInsert(c *gin.Context) {
	c.JSON(http.StatusOK, APIResponse{Success: true, Data: "Bulk insert performance - implementation needed"})
}