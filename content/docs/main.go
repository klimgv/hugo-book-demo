package main

import (
	"database/sql"
	"log"
	"math/rand"
	"sync"

	_ "github.com/mattn/go-sqlite3"
)

type User struct {
	Id   int
	Name string
	Age  int
	Ver  int
}

func main() {
	sqliteDb := initSqliteDb()
	defer sqliteDb.Close()

	fixture(sqliteDb)

	user := findUser(sqliteDb, 1)
	user2 := findUser(sqliteDb, 1)

	var wg sync.WaitGroup
	wg.Add(2)

	// 2 горутины пытаются обновить свои копии модели с определенной версией  __ver = 1, но только первая сможет обновить модель с данной версией
	go func() {
		defer wg.Done()
		randomAge := rand.Intn(100-1) + 1
		_, affected := updateUserAge(sqliteDb, user, randomAge)
		log.Println("affected: ", affected)
	}()

	go func() {
		defer wg.Done()
		randomAge := rand.Intn(100-1) + 1
		_, affected2 := updateUserAge(sqliteDb, user2, randomAge)
		log.Println("affected2: ", affected2)
	}()

	wg.Wait()
}

func findUser(db *sql.DB, id int) *User {
	stmt, err := db.Prepare("SELECT id, age, name, __ver FROM user WHERE id = ?")
	if err != nil {
		log.Fatal(err)
	}
	defer stmt.Close()

	row := stmt.QueryRow(id)
	if row.Err() != nil {
		return nil
	}

	model := User{}
	err = row.Scan(&model.Id, &model.Age, &model.Name, &model.Ver)
	if err != nil {
		log.Fatal(err)
	}

	return &model
}

func updateUserAge(db *sql.DB, model *User, age int) (error, int64) {
	stmt, err := db.Prepare(`UPDATE user SET age = ?, __ver = __ver + 1 WHERE id = ? AND __ver = ?`)
	if err != nil {
		log.Fatal(err)
	}
	defer stmt.Close()

	result, err := stmt.Exec(age, model.Id, model.Ver)
	if err != nil {
		return err, 0
	}

	count, err := result.RowsAffected()
	if err != nil {
		return err, 0
	}

	return nil, count
}

func initSqliteDb() *sql.DB {
	sqliteDb, err := sql.Open("sqlite3", "file::memory:?cache=shared")
	if err != nil {
		log.Fatal(err)
	}

	return sqliteDb
}

func fixture(db *sql.DB) {
	_, err := db.Exec(`DROP TABLE IF EXISTS user; CREATE TABLE user ("id" INTEGER PRIMARY KEY, "name" varchar(20), "age" INTEGER, "__ver" INTEGER)`)
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Exec(`INSERT INTO user ("id", "name", "age", "__ver") VALUES (1, "Alex", 35, 1), (2, "Bob", 23, 1)`)
	if err != nil {
		log.Fatal(err)
	}
}
