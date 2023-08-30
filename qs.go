package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"

	"github.com/itchyny/gojq"
)

const slowQueryFilter string = `select(.msg == "Slow query" and .c == "COMMAND" and .attr.command != "unrecognized" and (.attr.ninserted == 0 | not))`

const logToQuery string = `
(
	if .attr.command.q then 
	  .attr.command.q 
	elif .attr.command.query then 
	  .attr.command.query
	elif .attr.command.filter then
	  .attr.command.filter
	elif (.attr.command.updates | type) == "array" and
		 (.attr.command.updates | length) > 0 and 
		 (.attr.command.updates[0] | type) == "object" then
	  .attr.command.updates[0].q
	elif (.attr.command.deletes | type) == "array" and
		 (.attr.command.deletes | length) > 0 and 
		 (.attr.command.deletes[0] | type) == "object" then
	  .attr.command.deletes[0].q
	else
	  .attr.command.query
	end
  ) 
`

const queryToShape string = `
walk(
	if type == "array" then
	  sort | unique
	elif type == "object" then
	  if (.["$oid"] or .["$symbol"] or .["$numberInt"] or .["$numberLong"] or .["$numberDouble"] or .["$numberDecimal"]) then
		1
	  elif (.["$binary"] or .["$code"] or .["$timestamp"] or .["$regularExpression"] or .["$dbPointer"] or .["$date"])  then
		1
	  elif (.["$ref"] or .["$minKey"] or .["$maxKey"]) then
		1
	  elif .["$lt"] then
		.["$gt"] = .["$lt"] | del(.["$lt"])
	  elif .["$lte"] then
		.["$gt"] = .["$lte"] | del(.["$lte"])
	  elif .["$gte"] then
		.["$gt"] = .["$gte"] | del(.["$gte"])
	  else 
		.
	  end
	elif (type == "string" or type == "number" or type == "boolean") then 
	  1
	else 
	  . 
	end
  )
`

const logToNs string = `
  (
	if (.attr.ns != null) and (.attr.ns | test("\\$cmd$")) then
	  if .attr.command.find then
		"\(.attr.ns | split(".")[0]).\(.attr.command.find)"
	  elif .attr.command.findAndModify then
		"\(.attr.ns | split(".")[0]).\(.attr.command.findAndModify)"
	  elif .attr.command.update then
		"\(.attr.ns | split(".")[0]).\(.attr.command.update)"
	  elif .attr.command.insert then
		"\(.attr.ns | split(".")[0]).\(.attr.command.insert)"
	  elif .attr.command.delete then
		"\(.attr.ns | split(".")[0]).\(.attr.command.delete)"
	  else
		.attr.ns
	  end
	else
	  .attr.ns
	end
  )
`

const logToAction string = `
  (
	if .attr.type == "update" then  
	  "update" 
	elif .attr.type == "remove" then  
	  "remove" 
	elif .attr.command.find then
	  "find"
	elif .attr.command.findAndModify then 
	  "findAndModify" 
	elif .attr.command.update then 
	  "update" 
	elif .attr.command.insert then 
	  "insert" 
	elif .attr.command.delete then 
	  "delete" 
	else 
	  "other" 
	end
  )
`

const logToDurMs string = `.attr.durationMillis`

const logToShape string = logToQuery + ` | ` + queryToShape

const logToNsActionShapeDurMsObject string = `
    {
      "ns": ` + logToNs + `,
      "action": ` + logToAction + `,
      "shape": ` + logToShape + `,
      "durMS": ` + logToDurMs + "}"

const reduceWithDurMs string = `
  reduce inputs as $j
	({};
	$j.ns as $ns
	| $j.action as $action
	| $j.shape as $shape
	| $j.durMS as $durMS
	| .[$ns] as $curNsObj
	| "\($shape)" as $shapestr
	| $curNsObj[$shapestr] as $curShapeObj
	| ($curNsObj["countWithQuery"] + (if $shapestr == "null" or $shapestr == "{}" then 0 else 1 end)) as $newCountWithQuery
	| ($curNsObj["countWithoutQuery"] + (if $shapestr == "null" or $shapestr == "{}" then 1 else 0 end)) as $newCountWithoutQuery
	| $curShapeObj["actions"] as $curActionsObj
	| $curActionsObj[$action] as $curActionObj
	| {
		"count": ($curActionObj["count"] + 1),
		"durMSes": (if $curActionObj and $curActionObj["durMSes"] then $curActionObj["durMSes"] + [$durMS] else [$durMS] end)
		} as $setActionFields
	| ($curActionObj + $setActionFields) as $newActionObj
	| {
		($action): $newActionObj
		} as $setActionsFields
	| ($curActionsObj + $setActionsFields) as $newActionsObj
	| { 
		"count": ($curShapeObj["count"] + 1),
		"actions": $newActionsObj, 
		"shape": $shape 
		} 
		as $setShapeFields
	| ($curShapeObj + $setShapeFields) as $newShapeObj
	| { 
		($shapestr): $newShapeObj, 
		"countWithQuery": $newCountWithQuery, 
		"countWithoutQuery": $newCountWithoutQuery 
		} 
		as $setNsFields
	| ($curNsObj + $setNsFields) as $newNsObj
	| . + { ($ns): ($newNsObj) }
  )
`
const transformShapesToArray string = `
	to_entries 
	| map( 
		{
		"key": .key, 
		"value": {
			"countWithQuery": .value.countWithQuery,
			"countWithoutQuery": .value.countWithoutQuery,
			"queryShapes": (
				.value 
				| to_entries 
				| map(.value | objects)
			)
		}
		} 
	)
	| from_entries
`

const addStats string = `
	def ceil: if . | floor == . then . else . + 1.0 | floor end; 
	def perc($p; $arr): $arr | sort as $arr | ($arr | length) as $len | ($p / 100.0 * $len) | ceil as $rank | $arr[$rank - 1]; 
	walk(
	if type == "object" and has("durMSes") then 
		( 
		.p50 = perc(50; .durMSes) 
		| .p95 = perc(95; .durMSes) 
		| .max = (.durMSes | max) 
		| del(.durMSes) 
		) 
	else . 
	end
	)
`

const simplifiedLogs string = slowQueryFilter + ` | ` + logToNsActionShapeDurMsObject
const reduceAndStats string = reduceWithDurMs + ` | ` + transformShapesToArray + ` | ` + addStats

// channelIter struct
type channelIter struct {
	ch <-chan interface{}
}

// Next method to implement the Iter interface for channelIter
func (c *channelIter) Next() (interface{}, bool) {
	value, ok := <-c.ch
	return value, ok
}

func main() {
	// Create buffered channels with capacity 1000
	channel1 := make(chan interface{}, 1000)
	channel2 := make(chan interface{}, 1000)

	// Create a WaitGroup to wait for goroutines to complete
	var wg sync.WaitGroup

	// Increment the WaitGroup counter for each goroutine
	wg.Add(2)

	// Goroutine to read from channel1, do first gojq operation, and write to channel2
	go func() {
		query, err := gojq.Parse(simplifiedLogs)
		if err != nil {
			log.Fatalln(err)
		}
		code, err := gojq.Compile(query)
		if err != nil {
			log.Fatalln(err)
		}

		for message := range channel1 {
			//fmt.Fprintln(os.Stdout, "read from channel1: ", message)
			var jmap map[string]interface{}
			json.Unmarshal([]byte(message.(string)), &jmap)

			iter := code.Run(jmap)
			count := 0
			for {
				v, ok := iter.Next()
				if !ok {
					break
				}
				count++
				if count > 1 {
					fmt.Fprintln(os.Stderr, "Error: there should only be one result from code.Run")
				}
				if err, ok := v.(error); ok {
					fmt.Fprintln(os.Stderr, "error:", err)
				}

				channel2 <- v
			}

		}
		close(channel2)
		wg.Done()
	}()

	// Goroutine to read from channel2, do second gojq operation (reduce) and write to stdout
	go func() {
		query, err := gojq.Parse(reduceAndStats)
		if err != nil {
			fmt.Fprintln(os.Stderr, "Failed to parse jq query:", err)
			os.Exit(1)
		}

		chanIter := &channelIter{ch: channel2}

		code, err := gojq.Compile(query, gojq.WithInputIter(chanIter))
		if err != nil {
			fmt.Fprintln(os.Stderr, "Failed to compile jq query:", err)
			os.Exit(1)
		}

		// Create an iterator
		iter := code.Run(nil)

		// Execute the gojq query
		v, ok := iter.Next()
		if !ok {
			fmt.Fprintln(os.Stderr, "Error executing jq query.")
			os.Exit(1)
		}

		if err, ok := v.(error); ok {
			fmt.Fprintln(os.Stderr, "Error:", err)
			os.Exit(1)
		}

		encoder := json.NewEncoder(os.Stdout)

		// Print the final state containing statistics
		if err := encoder.Encode(v); err != nil {
			fmt.Fprintln(os.Stderr, "Error encoding final state:", err)
		}

		wg.Done()
	}()

	// Read from stdin and write to channel1
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		line := scanner.Text()
		//fmt.Fprintln(os.Stdout, "read from stdin: ", line)
		channel1 <- line
	}

	// Close channel1 to signal that no more data will be sent on it
	close(channel1)

	// Check for errors in stdin reading
	if err := scanner.Err(); err != nil {
		fmt.Fprintln(os.Stderr, "reading standard input:", err)
	}

	// Wait for all goroutines to complete
	wg.Wait()
}
