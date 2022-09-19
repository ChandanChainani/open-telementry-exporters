package main

import (
	"fmt"

	"database/sql"
	_ "github.com/mattn/go-sqlite3"
)

type Sqlite struct {
	db *sql.DB
}

func (e *Sqlite) Insert(query string, args ...interface{}) error {
	return e.doWithTx(func(tx *sql.Tx) error {
		statement, err := tx.Prepare(query)
		if err != nil {
			return fmt.Errorf("Prepare: %w", err)
		}
		defer statement.Close()

		_, err = statement.Exec(args...)
		if err != nil {
			return fmt.Errorf("ExecContext: %w", err)
		}
		return nil
	})
}

func (s *Sqlite) doWithTx(fn func(tx *sql.Tx) error) error {
	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("db.Begin: %w", err)
	}
	if err := fn(tx); err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit()
}

func (s *Sqlite) Close() error {
	return s.db.Close()
}

func SqlOpen(driverName, dataSourceName string) (Sqlite, error) {
	db, err := sql.Open("sqlite3", "test.db")
	return Sqlite{db}, err
}
