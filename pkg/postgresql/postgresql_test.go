package postgresql_test

import (
	"context"
	"os"
	"testing"

	"github.com/Liphium/hydro/hydrotest"
	"github.com/Liphium/hydro/pkg/postgresql"
	magic_pg "github.com/Liphium/magic/pkg/databases/postgres"
	"github.com/Liphium/magic/v3"
	"github.com/Liphium/magic/v3/mconfig"
	"github.com/stretchr/testify/assert"
	pg_driver "gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func TestMain(m *testing.M) {
	magic.PrepareTesting(m, magic.Config{
		AppName: "hydro-postgresql-driver",
		PlanDeployment: func(ctx *mconfig.Context) {
			driver := magic_pg.NewDriver("postgres:18").
				NewDatabase("pubsub")
			ctx.Register(driver)

			ctx.WithEnvironment(mconfig.Environment{
				"DB_HOST":     driver.Host(ctx),
				"DB_PORT":     driver.Port(ctx),
				"DB_USER":     driver.Username(),
				"DB_PASSWORD": driver.Password(),
				"DB_DATABASE": mconfig.ValueStatic("pubsub"),
			})
		},
		StartFunction: magic.AppStarted,
	})
}

func TestPubSub(t *testing.T) {
	url := "host=" + os.Getenv("DB_HOST") + " user=" + os.Getenv("DB_USER") + " password=" + os.Getenv("DB_PASSWORD") + " dbname=" + os.Getenv("DB_DATABASE") + " port=" + os.Getenv("DB_PORT") + " sslmode=disable"

	db, err := gorm.Open(pg_driver.Open(url), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Warn),
	})
	assert.NoError(t, err)

	hydrotest.TestPubSubBackend(t, func() *postgresql.PostgresPubSub {
		return postgresql.NewPostgresPubSub(url)
	}, func(backend *postgresql.PostgresPubSub, c context.Context, channel, message string) error {
		return backend.Publish(c, db, channel, message)
	})
}
