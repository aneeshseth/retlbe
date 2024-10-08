package inputs

import (
	"os"
	"retl/inputs/postgres"
	"retl/inputs/snowflake"
	"retl/inputs/types"
)



func Start() {
	var inputs map[string]Input = map[string]Input{
		"snowflake": &snowflake.Snowflake{
			Conf: &types.ConfigType{
				Settings: map[string]interface{}{
					"org": os.Getenv("SNOWFLAKE_ORG"),
					"acc": os.Getenv("SNOWFLAKE_ACC"),
					"db": os.Getenv("SNOWFLAKE_DB"),
					"wh": os.Getenv("SNOWFLAKE_WH"),
				},
				Secrets: map[string]interface{}{
					"username": os.Getenv("SNOWFLAKE_USERNAME"),
					"password": os.Getenv("SNOWFLAKE_PASSWORD"),
				},
			},
		},
		"postgres": &postgres.Postgres{
			Conf: &types.ConfigType{
				Settings: map[string]interface{}{
					"table": os.Getenv("POSTGRES_TABLE"),
					"filter": os.Getenv("POSTGRES_FILTER"),
				},
				Secrets: map[string]interface{}{
					"url": os.Getenv("POSTGRES_URL"),
				},
			},
		},
	}
	CONNECTOR_NAME := os.Getenv("CONNECTOR_NAME")
	for key, value := range inputs {
		if CONNECTOR_NAME == key {
			value.Run()
		}
	}
}