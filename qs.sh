SLOW_CMD='select(.msg == "Slow query" and .c == "COMMAND" and .attr.command != "unrecognized" and (.attr.ninserted == 0 | not))'
SLOW_CMD_LIMIT_10="limit(10; $SLOW_CMD)"

LOG_TO_QUERY='
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
'

QUERY_TO_SHAPE='
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
'

# note: findAndModify also has .attr.command.update, so findAndModify must be checked first
LOG_TO_NS='
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
'

# note: findAndModify also has .attr.command.update, so findAndModify must be checked first
LOG_TO_ACTION='
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
'

LOG_TO_DURMS='.attr.durationMillis'



LOG_TO_SHAPE="($LOG_TO_QUERY | $QUERY_TO_SHAPE)"


LOG_TO_NS_ACTION_SHAPE_OBJECT='
    {
      "ns": '
LOG_TO_NS_ACTION_SHAPE_OBJECT+=$LOG_TO_NS
LOG_TO_NS_ACTION_SHAPE_OBJECT+=',
      "action": '
LOG_TO_NS_ACTION_SHAPE_OBJECT+=$LOG_TO_ACTION
LOG_TO_NS_ACTION_SHAPE_OBJECT+=',
      "shape": '
LOG_TO_NS_ACTION_SHAPE_OBJECT+=$LOG_TO_SHAPE
LOG_TO_NS_ACTION_SHAPE_OBJECT+="}"   


NS_ACTION_SHAPE_OBJECT_TO_STR='"\(.ns)|\(.action)|\(.shape)"'



LOG_TO_NS_ACTION_SHAPE_DURMS_OBJECT='
    {
      "ns": '
LOG_TO_NS_ACTION_SHAPE_DURMS_OBJECT+=$LOG_TO_NS
LOG_TO_NS_ACTION_SHAPE_DURMS_OBJECT+=',
      "action": '
LOG_TO_NS_ACTION_SHAPE_DURMS_OBJECT+=$LOG_TO_ACTION
LOG_TO_NS_ACTION_SHAPE_DURMS_OBJECT+=',
      "shape": '
LOG_TO_NS_ACTION_SHAPE_DURMS_OBJECT+=$LOG_TO_SHAPE
LOG_TO_NS_ACTION_SHAPE_DURMS_OBJECT+=',
      "durMS": '
LOG_TO_NS_ACTION_SHAPE_DURMS_OBJECT+=$LOG_TO_DURMS
LOG_TO_NS_ACTION_SHAPE_DURMS_OBJECT+="}"   






REDUCE='
  reduce inputs as $j
    ({};
     $j.ns as $ns
     | $j.action as $action
     | $j.shape as $shape
     | .[$ns] as $curNsObj
     | "\($shape)" as $shapestr
     | $curNsObj[$shapestr] as $curShapeObj
     | ($curNsObj["countWithQuery"] + (if $shapestr == "null" or $shapestr == "{}" then 0 else 1 end)) as $newCountWithQuery
     | ($curNsObj["countWithoutQuery"] + (if $shapestr == "null" or $shapestr == "{}" then 1 else 0 end)) as $newCountWithoutQuery
     | $curShapeObj["actions"] as $curActionsObj
     | {
         ($action): ($curShapeObj["actions"][$action] + 1)
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
'

REDUCE_WITH_DURMS='
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
'


TRANSFORM_SHAPES_TO_ARRAY='
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
| from_entries'

ADD_STATS='
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
'


#cat $1 | jq -c "$SLOW_CMD | $LOG_TO_NS_ACTION_SHAPE_OBJECT" | jq --sort-keys | jq -n "$REDUCE | $TRANSFORM_SHAPES_TO_ARRAY" | jq --sort-keys

cat $1 | jq -c "$SLOW_CMD | $LOG_TO_NS_ACTION_SHAPE_DURMS_OBJECT" | jq --sort-keys | jq -n "$REDUCE_WITH_DURMS | $TRANSFORM_SHAPES_TO_ARRAY" | jq --sort-keys | jq "$ADD_STATS"

# cat $1 | jq -c "$SLOW_CMD | $LOG_TO_NS_ACTION_SHAPE_OBJECT" | head -n 1000 | jq --sort-keys | jq -n "$REDUCE"


#          
