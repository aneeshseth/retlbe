package db

import (
	"os"

	"github.com/supabase-community/supabase-go"
)

func NewClient() (*supabase.Client, error) {
	client, err := supabase.NewClient(os.Getenv("SUPABASE_API_URL"), os.Getenv("SUPABASE_API_KEY"), &supabase.ClientOptions{})
	if err != nil {
	   return nil, err
	}
	return client, nil
}