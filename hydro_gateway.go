package hydro

import "github.com/gofiber/fiber/v2"

// Create a gateway for the Hydro instance
func (i *Instance[T, PS]) Gateway() func(c *fiber.Ctx) error {
	return func(c *fiber.Ctx) error {
		return nil
	}
}
