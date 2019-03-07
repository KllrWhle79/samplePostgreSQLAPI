package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/gorilla/mux"
	_ "github.com/lib/pq"
	"log"
	"net/http"
)

var DB *sql.DB

const (
	dbhost     = "localhost"
	dbport     = "5432"
	dbuser     = "user"
	dbpassword = "password"
	dbname     = "test_db"
)

type Test1Row struct {
	TestInt int    `json:"test_int1"`
	TestStr string `json:"test_string"`
}

func main() {
	initDB()
	defer DB.Close()

	router := mux.NewRouter()

	router.HandleFunc("/api/list", listHandler).Methods(http.MethodGet)
	router.HandleFunc("/api/{table}", writeHandler).Methods(http.MethodPost)
	router.HandleFunc("/api/{table}", readHandler).Methods(http.MethodGet)
	log.Fatal(http.ListenAndServe("localhost:8000", router))
}

func initDB() {
	var err error
	psqlInfo := fmt.Sprintf("host=%s port=%s user=%s "+
		"password=%s dbname=%s sslmode=disable",
		dbhost, dbport, dbuser, dbpassword, dbname)
	DB, err = sql.Open("postgres", psqlInfo)
	if err != nil {
		panic(err)
	}
	err = DB.Ping()
	if err != nil {
		panic(err)
	}
	fmt.Println("Successfully connected to DB!")
}

// listHandler objects in database
func listHandler(writer http.ResponseWriter, request *http.Request) {
	tableList, err := DB.Query("SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname='public'")
	if err != nil {
		panic(err)
	}

	if tableList.Err() != nil {
		http.Error(writer, tableList.Err().Error(), 500)
		return
	}

	var tables []string
	for tableList.Next() {
		var tblName string
		err = tableList.Scan(&tblName)
		tables = append(tables, tblName)
	}

	if err != nil {
		http.Error(writer, err.Error(), 500)
		return
	}

	fmt.Fprintf(writer, fmt.Sprintf("%v", tables))
}

func writeHandler(writer http.ResponseWriter, request *http.Request) {
	params := mux.Vars(request)
	var testRow Test1Row
	_ = json.NewDecoder(request.Body).Decode(&testRow)
	sqlInsert := fmt.Sprintf("INSERT INTO %s VALUES (%d, '%s')",
		params["table"], testRow.TestInt, testRow.TestStr)
	_, err := DB.Query(sqlInsert)
	if err != nil {
		http.Error(writer, err.Error(), 500)
	}
}

func readHandler(writer http.ResponseWriter, request *http.Request) {
	params := mux.Vars(request)
	queryString := request.URL.Query()
	sqlQuery := fmt.Sprintf("SELECT * FROM %s", params["table"])
	sqlWhere := ""
	for key, val := range queryString {
		if key == "test_int1" {
			if sqlWhere != "" {
				sqlWhere += " AND"
			} else {
				sqlWhere += " WHERE"
			}
			sqlWhere += fmt.Sprintf(" test_int1=%s", val[0])
		} else if key == "test_string" {
			if sqlWhere != "" {
				sqlWhere += " AND"
			} else {
				sqlWhere += " WHERE"
			}
			sqlWhere += fmt.Sprintf(" test_string='%s'", val[0])
		}
	}

	rows, err := DB.Query(sqlQuery+sqlWhere)
	if err != nil {
		http.Error(writer, err.Error(), 500)
	}
	defer rows.Close()

	var output []byte
	var testRow Test1Row
	for rows.Next() {
		err := rows.Scan(&testRow.TestInt, &testRow.TestStr)
		if err != nil {
			panic(err)
		}
		output, _ = json.MarshalIndent(testRow, "", "  ")
	}

	fmt.Fprintf(writer, string(output))
}