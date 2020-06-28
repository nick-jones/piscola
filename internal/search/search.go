package search

import (
	"context"
	"log"

	"github.com/nick-jones/piscola/internal/gen-go/service"
)

type Service struct {
	db *Database
}

func NewService() *Service {
	return &Service{
		db: newDatabase(),
	}
}

func (s *Service) Lookup(_ context.Context, query *service.Query) (*service.Result_, error) {
	log.Printf("lookup: %v", query)
	return s.db.Lookup(query)
}

func (s *Service) LookupAdvanced(_ context.Context, query *service.AdvancedQuery) (*service.Result_, error) {
	log.Printf("lookup advanced: %v", query)
	return s.db.LookupAdvanced(query)
}

func (s *Service) Add(_ context.Context, item *service.Item) (bool, error) {
	return s.db.Add(item), nil
}

func (s *Service) Replace(_ context.Context, item *service.Item) (bool, error) {
	return s.db.Put(item), nil
}

func (s *Service) Remove(_ context.Context, id int32) (bool, error) {
	return s.db.Remove(id), nil
}
