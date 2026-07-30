package starter

import (
	"fmt"

	"github.com/Liphium/magic/pkg/databases/postgres"
	"github.com/Liphium/magic/v3"
	"github.com/Liphium/magic/v3/mconfig"
	"github.com/Liphium/magic/v3/scripting"
)

func BuildMagicConfig() magic.Config {
	return magic.Config{
		AppName: "hydro-example-real-project",
		PlanDeployment: func(ctx *mconfig.Context) {

			// Create a new driver for PostgreSQL databases
			driver := postgres.NewDriver("postgres:18").
				// Create a PostgreSQL database for the posts service (the driver supports a builder pattern with this method)
				NewDatabase("posts")

			// Make sure to register the driver in the context
			ctx.Register(driver)

			// Allocate a new port for the service. This makes it possible to run multiple instances of this app
			// locally, without weird configuration hell. Magic will pick a port in case the preferred one is taken.
			port := ctx.ValuePort(8080)

			// Set up environment variables for the application
			ctx.WithEnvironment(mconfig.Environment{
				// Database connection environment variables
				"DB_HOST":     driver.Host(ctx),
				"DB_PORT":     driver.Port(ctx),
				"DB_USER":     driver.Username(),
				"DB_PASSWORD": driver.Password(),
				"DB_DATABASE": mconfig.ValueStatic("posts"),

				// Make the server listen on localhost using the port allocated by Magic
				"LISTEN": mconfig.ValueWithBase([]mconfig.EnvironmentValue{port}, func(s []string) string {
					return fmt.Sprintf("127.0.0.1:%s", s[0])
				}),
			})

			// Load any additional secrets from a .env file if it exists, you could use this to load additional credentials
			// for services Magic might not support (yet c:).
			// _ = ctx.LoadSecretsToEnvironment(".env")
		},
		StartFunction: Start,
		Scripts: []scripting.Script{
			// Scripts to call endpoints, really useful for tests and development
			scripting.CreateScript("create-post", "Create a post using the endpoint", CreatePost),
			scripting.CreateScript("list-posts", "List posts using the endpoint", PrintPosts),
			scripting.CreateScript("update-post", "Update a post using the endpoint", UpdatePost),
			scripting.CreateScript("subscribe-post", "Subscribe to post updates", SubscribeToPost),
		},
	}
}
