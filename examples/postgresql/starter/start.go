package starter

import (
	"bufio"
	"encoding/json"
	"examples/simple/database"
	"examples/simple/pubsub"
	"fmt"
	"os"
	"strings"

	"github.com/Liphium/hydro"
	"github.com/Liphium/magic/v3"
	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/valyala/fasthttp"
	"gorm.io/gorm"
)

func Start() {

	// Connect to the database
	database.Connect()

	// Setup Hydro
	url := "host=" + os.Getenv("DB_HOST") + " user=" + os.Getenv("DB_USER") + " password=" + os.Getenv("DB_PASSWORD") + " dbname=" + os.Getenv("DB_DATABASE") + " port=" + os.Getenv("DB_PORT") + " sslmode=disable"
	ps := pubsub.NewPostgresPubSub(url)
	h := hydro.New[*gorm.DB, *pubsub.PostgresPubSub](&hydro.Config[*gorm.DB, *pubsub.PostgresPubSub]{
		PubSubBackend: ps,
	})

	// Setup ListenerDictionary
	ld := hydro.NewListenerDictionary[*gorm.DB, *pubsub.PostgresPubSub, *database.Post](h, hydro.DatabaseListenerCreate[*gorm.DB, *database.Post]{
		Identifier: "posts",
		Get: func(db *gorm.DB, keys []string) (map[string]*database.Post, error) {
			results := make(map[string]*database.Post)
			for _, key := range keys {
				id := key
				if strings.HasPrefix(key, "ld:posts:") {
					id = key[9:]
				}

				var post database.Post
				if err := db.First(&post, "id = ?", id).Error; err == nil {
					results[key] = &post
				}
			}
			return results, nil
		},
	})

	// Create the actual web app
	app := fiber.New()

	// This message is just here for explanation.
	fmt.Println()
	fmt.Println("Welcome, wizard! That's how easy it is to get Magic up and running. Well, I hope it actually all went well for you...")
	fmt.Println("Anyway, now that we're up and running, you can open another terminal and run scripts using: 'go run . -r <script>' or list of all of them using 'go run . --scripts'!")
	fmt.Println("Thanks for using Magic!")

	// Basic insertion endpoint to create a new post
	app.Post("/posts", func(c *fiber.Ctx) error {
		var post database.Post
		if err := c.BodyParser(&post); err != nil {
			return c.Status(400).JSON(fiber.Map{"error": "Invalid JSON format"})
		}
		if post.Author == "" || post.Content == "" {
			return c.Status(400).JSON(fiber.Map{"error": "Author and content are required"})
		}

		err := database.DBConn.Transaction(func(tx *gorm.DB) error {
			if err := tx.Create(&post).Error; err != nil {
				return err
			}
			return ld.Update(c.Context(), tx, post.ID.String(), &post)
		})

		if err != nil {
			return c.Status(500).JSON(fiber.Map{"error": err.Error()})
		}
		return c.Status(fiber.StatusCreated).JSON(post)
	})

	// Update existing post
	app.Post("/posts/:id", func(c *fiber.Ctx) error {
		id := c.Params("id")
		var update struct {
			Content string `json:"content"`
		}
		if err := c.BodyParser(&update); err != nil {
			return c.Status(400).JSON(fiber.Map{"error": "Invalid JSON"})
		}

		var post database.Post
		err := database.DBConn.Transaction(func(tx *gorm.DB) error {
			if err := tx.First(&post, "id = ?", id).Error; err != nil {
				return err
			}
			post.Content = update.Content
			if err := tx.Save(&post).Error; err != nil {
				return err
			}
			return ld.Update(c.Context(), tx, post.ID.String(), &post)
		})

		if err != nil {
			return c.Status(500).JSON(fiber.Map{"error": err.Error()})
		}
		return c.JSON(post)
	})

	// SSE Endpoint
	app.Get("/sub/:id", func(c *fiber.Ctx) error {
		id := c.Params("id")
		if _, err := uuid.Parse(id); err != nil {
			return c.Status(400).SendString("Invalid UUID")
		}

		c.Set("Content-Type", "text/event-stream")
		c.Set("Cache-Control", "no-cache")
		c.Set("Connection", "keep-alive")
		c.Set("Transfer-Encoding", "chunked")

		c.Context().SetBodyStreamWriter(fasthttp.StreamWriter(func(w *bufio.Writer) {
			msgs := make(chan string, 10)
			
			// Subscription implementation for ListenerDictionary
			subID := uuid.New().String()
			err := ld.Subscribe(database.DBConn, []string{id}, subID, func(change hydro.Change[*database.Post]) {
				post := change.(*database.Post)
				data, _ := json.Marshal(post)
				msgs <- string(data)
			})
			if err != nil {
				return
			}
			defer ld.Unsubscribe(subID, []string{id})

			for {
				select {
				case <-c.Context().Done():
					return
				case msg := <-msgs:
					fmt.Fprintf(w, "data: %s\n\n", msg)
					if err := w.Flush(); err != nil {
						return
					}
				}
			}
		}))

		return nil
	})

	// Basic get endpoint to get all posts
	app.Get("/posts", func(c *fiber.Ctx) error {
		var posts []database.Post
		if err := database.DBConn.Find(&posts).Error; err != nil {
			return c.Status(500).JSON(fiber.Map{
				"error": "Failed to retrieve posts",
			})
		}
		return c.JSON(posts)
	})

	// Basic get endpoint to get a single post
	app.Get("/posts/:id", func(c *fiber.Ctx) error {
		id := c.Params("id")

		// Validate UUID format
		if _, err := uuid.Parse(id); err != nil {
			return c.Status(400).JSON(fiber.Map{
				"error": "Invalid UUID format",
			})
		}

		var post database.Post
		if err := database.DBConn.First(&post, "id = ?", id).Error; err != nil {
			if err == gorm.ErrRecordNotFound {
				return c.Status(404).JSON(fiber.Map{
					"error": "Post not found",
				})
			}
			return c.Status(500).JSON(fiber.Map{
				"error": "Failed to retrieve post",
			})
		}

		return c.JSON(post)
	})

	// Add a startup hook to notify Magic of the app start
	app.Hooks().OnListen(func(listenData fiber.ListenData) error {
		if fiber.IsChild() {
			return nil
		}

		// Tell Magic the app has started: Makes sure tests start to run after this.
		magic.AppStarted()
		return nil
	})

	app.Listen(os.Getenv("LISTEN"))
}
