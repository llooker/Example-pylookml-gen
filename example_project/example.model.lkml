connection: "my_great_bq"

explore: view1 {
    join: view2 {
        sql_on: ${view1.id} = ${view2.fk} ;;
    }
}