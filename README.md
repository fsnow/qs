# qs

qs ("query shapes") is a log-parsing library for MongoDB logs focusing on logged queries. There are two implementations, one using jq called from a bash script (qs.sh) and an equivalent golang application that uses the gojq library.

Pre-requisites:
bash and jq, or go

To run the bash/jq version:
```
chmod +x qs.sh
./qs.sh < yourLogFile.log
```

To run the golang version:
```
go run qs.go < yourLogFile.log
```

To compile the golang version and run the binary:
```
go build
./qs < yourLogFile.log
```


Example output:
```
{
  "testdb.testcoll": {
    "countWithQuery": 4,
    "countWithoutQuery": 0,
    "queryShapes": [
      {
        "actions": {
          "find": {
            "count": 2,
            "durationMillis": {
              "max": 2,
              "p50": 0,
              "p95": 2
            }
          },
          "update": {
            "count": 1,
            "durationMillis": {
              "max": 94,
              "p50": 94,
              "p95": 94
            }
          }
        },
        "count": 3,
        "shape": {
          "a": 1
        }
      },
      {
        "actions": {
          "update": {
            "count": 1,
            "durationMillis": {
              "max": 1,
              "p50": 1,
              "p95": 1
            }
          }
        },
        "count": 1,
        "shape": {
          "_id": 1,
          "a": 1
        }
      }
    ]
  }
}
```
