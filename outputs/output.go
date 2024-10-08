package outputs

import (
	"os"
	"retl/outputs/algolia"
	"retl/outputs/types"
)



func Start() {
	var outputs map[string]Output = map[string]Output{
		"algolia": &algolia.Algolia{
			Conf: &types.ConfigType{
				Settings: map[string]interface{}{
					"index": os.Getenv("ALGOLIA_INDEX"),
				},
				Secrets: map[string]interface{}{
					"app_id": os.Getenv("ALGOLIA_APP_ID"),
					"api_key": os.Getenv("ALGOLIA_API_KEY"),
				},
			},
		},
	}
	CONNECTOR_NAME := os.Getenv("CONNECTOR_NAME")
	for key, value := range outputs {
		if CONNECTOR_NAME == key {
			value.Run()
		}
	}
}