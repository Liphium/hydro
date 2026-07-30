package starter

import (
	"encoding/json"
	"examples/simple/database"
	"fmt"
	"os"

	"github.com/gofiber/fiber/v2"
	"resty.dev/v3"
)

// This method would ideally be created in a shared package between all the scripts.
func GetPath() string {
	return fmt.Sprintf("http://%s", os.Getenv("LISTEN"))
}

// Script for creating a post using the endpoint.
//
// You could go into the database and add it there, but we want to be able to call the endpoint using scripts.
func CreatePost(post database.Post) error {
	client := resty.New()
	defer client.Close()

	_, err := client.R().
		SetBody(post).
		Post(GetPath() + "/posts")
	return err
}

// Script for printing all the posts using the endpoint.
func PrintPosts() error {
	client := resty.New()
	defer client.Close()

	res, err := client.R().
		Get(GetPath() + "/posts")
	if err != nil || res.StatusCode() != fiber.StatusOK {
		return fmt.Errorf("couldn't get posts: %v", err)
	}

	var posts []database.Post
	if err := json.Unmarshal(res.Bytes(), &posts); err != nil {
		return err
	}

	better, _ := json.MarshalIndent(posts, "", "   ")
	fmt.Println(string(better))
	return nil
}

type PostUpdate struct {
	ID      string
	Content string
}

// Update post via endpoint
func UpdatePost(post PostUpdate) error {
	client := resty.New()
	defer client.Close()

	_, err := client.R().
		SetBody(map[string]string{"content": post.Content}).
		Post(GetPath() + "/posts/" + post.ID)
	return err
}

// Subscribe to post updates via SSE
func SubscribeToPost(id struct {
	ID string
}) error {
	resty.NewSSESource().
		SetURL(GetPath()+"/sub/"+id.ID).
		OnMessage(func(e any) {
			fmt.Println(e.(*resty.SSE))
		}, nil)

	select {}
}
