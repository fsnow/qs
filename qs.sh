SLOW_QUERY='select(.msg == "Slow query")'
SLOW_QUERY_LIMIT_10="limit(10; $SLOW_QUERY)"

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

LOG_TO_ACTION='
(
  if .attr.type == "update" then  
    "update" 
  elif .attr.type == "remove" then  
    "remove" 
  elif .attr.command.find then
    "find"
  elif .attr.command.update then 
    "update" 
  elif .attr.command.findAndModify then 
    "findAndModify" 
  elif .attr.command.insert then 
    "insert" 
  elif .attr.command.delete then 
    "delete" 
  else 
    "other" 
  end
)
'

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


REDUCE1='
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
     | { 
         "count": ($curShapeObj["count"] + 1),
         "percentage": (if $newCountWithQuery == 0 then 0 else (((($curShapeObj["count"] + 1) / $newCountWithQuery * 10000) | round) / 100) end),
         ($action): ($curShapeObj[$action] + 1), 
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


cat $1 | jq -c "$SLOW_QUERY | $LOG_TO_NS_ACTION_SHAPE_OBJECT" | jq -n "$REDUCE1"