[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Replace"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared #"Traffic Accidents" = let
  Source = Excel.Workbook(Web.Contents("https://statbel.fgov.be/sites/default/files/files/opendata/Accident_XY/OPENDATA_MAP_2017-2022.xlsx"), null, true),
  #"Navigation 1" = Source{[Item = "Feuil1", Kind = "Sheet"]}[Data],
  #"Promoted headers" = Table.PromoteHeaders(#"Navigation 1", [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"DT_YEAR_COLLISION", Int64.Type}, {"DT_MONTH_COLLISION", Int64.Type}, {"DT_TIME", Int64.Type}, {"CD_NIS", Int64.Type}, {"TX_RGN_COLLISION_FR", type text}, {"TX_RGN_COLLISION_NL", type text}, {"TX_PROV_COLLISION_FR", type text}, {"TX_PROV_COLLISION_NL", type text}, {"TX_MUNTY_COLLISION_FR", type text}, {"TX_MUNTY_COLLISION_NL", type text}, {"MS_X_COORD", type number}, {"MS_Y_COORD", type number}, {"CD_CROSSWAY", Int64.Type}, {"TX_CROSSWAY_FR", type text}, {"TX_CROSSWAY_NL", type text}, {"CD_WEATHER", type text}, {"TX_WEATHER_FR", type text}, {"TX_WEATHER_NL", type text}, {"CD_ROAD_CONDITION", type text}, {"TX_ROAD_CONDITION_FR", type text}, {"TX_ROAD_CONDITION_NL", type text}, {"CD_BUILD_UP_AREA", Int64.Type}, {"TX_BUILD_UP_AREA_FR", type text}, {"TX_BUILD_UP_AREA_NL", type text}, {"CD_LIGHT_CONDITION", Int64.Type}, {"TX_LIGHT_CONDITION_FR", type text}, {"TX_LIGHT_CONDITION_NL", type text}, {"CD_ROAD_TYPE", Int64.Type}, {"CD_ROAD_TYPE_FR", type text}, {"CD_ROAD_TYPE_NL", type text}, {"CD_CLASS_ACCIDENTS", Int64.Type}, {"TX_CLASS_ACCIDENTS_FR", type text}, {"TX_CLASS_ACCIDENTS_NL", type text}, {"CD_ROAD_USR_TYPE1", Int64.Type}, {"TX_ROAD_USR_TYPE1_FR", type text}, {"TX_ROAD_USR_TYPE1_NL", type text}, {"CD_ROAD_USR_TYPE2", Int64.Type}, {"TX_ROAD_USR_TYPE2_FR", type text}, {"TX_ROAD_USR_TYPE2_NL", type text}, {"CD_COLLISION_TYPE", type text}, {"TX_COLLISON_TYPE_FR", type text}, {"TX_COLLISION_TYPE_NL", type text}, {"CD_OBSTACLES", Int64.Type}, {"TX_OBSTACLES_FR", type text}, {"TX_OBSTACLES_NL", type text}}, "en"),
  #"Renamed columns" = Table.RenameColumns(#"Changed column type", {{"DT_YEAR_COLLISION", "year"}, {"DT_MONTH_COLLISION", "month"}, {"DT_TIME", "time"}, {"CD_NIS", "nis_code"}, {"TX_RGN_COLLISION_FR", "region_fr"}, {"TX_RGN_COLLISION_NL", "region_nl"}, {"TX_PROV_COLLISION_FR", "_province_fr"}, {"TX_PROV_COLLISION_NL", "_province_nl"}, {"TX_MUNTY_COLLISION_FR", "municipality_fr"}, {"TX_MUNTY_COLLISION_NL", "municipality_nl"}, {"MS_X_COORD", "x_coord_belgian_lambert"}, {"MS_Y_COORD", "y_coord_belgian_lambert"}, {"CD_CROSSWAY", "crossway_id"}, {"TX_CROSSWAY_FR", "crossway_fr"}, {"TX_CROSSWAY_NL", "crossway_nl"}, {"CD_WEATHER", "weather_id"}, {"TX_WEATHER_FR", "weather_fr"}, {"TX_WEATHER_NL", "weather_nl"}, {"CD_ROAD_CONDITION", "road_condition_id"}, {"TX_ROAD_CONDITION_FR", "road_condition_fr"}, {"TX_ROAD_CONDITION_NL", "road_condition_nl"}, {"CD_BUILD_UP_AREA", "build_up_area_id"}, {"TX_BUILD_UP_AREA_FR", "build_up_area_fr"}, {"TX_BUILD_UP_AREA_NL", "build_up_area_nl"}, {"CD_LIGHT_CONDITION", "light_condition_id"}, {"TX_LIGHT_CONDITION_FR", "light_condition_fr"}, {"TX_LIGHT_CONDITION_NL", "light_condition_nl"}, {"CD_ROAD_TYPE", "road_type_id"}, {"CD_ROAD_TYPE_FR", "road_type_fr"}, {"CD_ROAD_TYPE_NL", "road_type_nl"}, {"CD_CLASS_ACCIDENTS", "accident_type_id"}, {"TX_CLASS_ACCIDENTS_FR", "accident_type_fr"}, {"TX_CLASS_ACCIDENTS_NL", "accident_type_nl"}, {"CD_ROAD_USR_TYPE1", "first_road_user_id"}, {"TX_ROAD_USR_TYPE1_FR", "first_road_user_fr"}, {"TX_ROAD_USR_TYPE1_NL", "first_road_user_nl"}, {"CD_ROAD_USR_TYPE2", "second_road_user_id"}, {"TX_ROAD_USR_TYPE2_FR", "second_road_user_fr"}, {"TX_ROAD_USR_TYPE2_NL", "second_road_user_nl"}, {"CD_COLLISION_TYPE", "collision_type_id"}, {"TX_COLLISON_TYPE_FR", "collision_type_fr"}, {"TX_COLLISION_TYPE_NL", "collision_type_nl"}, {"CD_OBSTACLES", "obstacle_id"}, {"TX_OBSTACLES_FR", "obstacle_fr"}, {"TX_OBSTACLES_NL", "obstacle_nl"}}),
  #"correct weather id" = Table.ReplaceValue(#"Renamed columns", "/", "", Replacer.ReplaceText, {"weather_id"}),
  #"correct road condition id" = Table.ReplaceValue(#"correct weather id", "/", "", Replacer.ReplaceText, {"road_condition_id"}),
  #"correct collision type id" = Table.ReplaceValue(#"correct road condition id", "A", "99", Replacer.ReplaceText, {"collision_type_id"}),
  #"Changed column type 1" = Table.TransformColumnTypes(#"correct collision type id", {{"collision_type_id", Int64.Type}, {"road_condition_id", Int64.Type}, {"weather_id", Int64.Type}}),
  #"clean province fr" = Table.AddColumn(#"Changed column type 1", "province_fr", each let splitregionfr = Splitter.SplitTextByDelimiter(" ", QuoteStyle.None)([region_fr]), splitprovincefr = Splitter.SplitTextByDelimiter(" ", QuoteStyle.None)([_province_fr]) in Text.Combine({splitregionfr{2}?, Text.Middle(splitprovincefr{1}?, 2, 1), Text.Middle([_province_fr], 12)}), type text),
  #"clean province nl" = Table.AddColumn(#"clean province fr", "province_nl", each let splitprovincenl = Splitter.SplitTextByDelimiter("Provincie ", QuoteStyle.None)([_province_nl]), splitregionnl = Splitter.SplitTextByDelimiter("Vlaams ", QuoteStyle.None)([region_nl]) in Text.Combine({Text.Combine(splitprovincenl), splitregionnl{0}?}), type text),
  #"add latitude" = Table.TransformColumnTypes(Table.AddColumn(#"clean province nl", "latitude", each [y_coord_belgian_lambert]), {{"latitude", type number}}),
  #"add longitude" = Table.TransformColumnTypes(Table.AddColumn(#"add latitude", "longitude", each [x_coord_belgian_lambert]), {{"longitude", type number}})
in
  #"add longitude";
[BindToDefaultDestination = true]
shared accident_type = let
  Source = #"Traffic Accidents",
  #"Choose columns" = Table.SelectColumns(Source, {"accident_type_id", "accident_type_fr", "accident_type_nl"}),
  #"Marked key columns" = Table.AddKey(#"Choose columns", {"accident_type_id"}, false),
  #"Removed duplicates" = Table.Distinct(#"Marked key columns", {"accident_type_id", "accident_type_fr", "accident_type_nl"})
in
  #"Removed duplicates";
[BindToDefaultDestination = true]
shared build_up_area = let
  Source = #"Traffic Accidents",
  #"Choose columns" = Table.SelectColumns(Source, {"build_up_area_id", "build_up_area_fr", "build_up_area_nl"}),
  #"Marked key columns" = Table.AddKey(#"Choose columns", {"build_up_area_id"}, false),
  #"Removed duplicates" = Table.Distinct(#"Marked key columns", {"build_up_area_id", "build_up_area_fr", "build_up_area_nl"})
in
  #"Removed duplicates";
[BindToDefaultDestination = true]
shared collision_type = let
  Source = #"Traffic Accidents",
  #"Choose columns" = Table.SelectColumns(Source, {"collision_type_id", "collision_type_fr", "collision_type_nl"}),
  #"Marked key columns" = Table.AddKey(#"Choose columns", {"collision_type_id"}, false),
  #"Removed duplicates" = Table.Distinct(#"Marked key columns", {"collision_type_id", "collision_type_fr", "collision_type_nl"})
in
  #"Removed duplicates";
[BindToDefaultDestination = true]
shared crossway = let
  Source = #"Traffic Accidents",
  #"Choose columns" = Table.SelectColumns(Source, {"crossway_id", "crossway_fr", "crossway_nl"}),
  #"Marked key columns" = Table.AddKey(#"Choose columns", {"crossway_id"}, false),
  #"Removed duplicates" = Table.Distinct(#"Marked key columns", {"crossway_id", "crossway_fr", "crossway_nl"})
in
  #"Removed duplicates";
[BindToDefaultDestination = true]
shared light_condition = let
  Source = #"Traffic Accidents",
  #"Choose columns" = Table.SelectColumns(Source, {"light_condition_id", "light_condition_fr", "light_condition_nl"}),
  #"Marked key columns" = Table.AddKey(#"Choose columns", {"light_condition_id"}, false),
  #"Removed duplicates" = Table.Distinct(#"Marked key columns", {"light_condition_id", "light_condition_fr", "light_condition_nl"})
in
  #"Removed duplicates";
[BindToDefaultDestination = true]
shared location = let
  Source = #"Traffic Accidents",
  #"Choose columns" = Table.SelectColumns(Source, {"nis_code", "region_fr", "region_nl", "municipality_fr", "municipality_nl", "province_fr", "province_nl"}),
  #"Marked key columns" = Table.AddKey(#"Choose columns", {"nis_code"}, false),
  #"Removed duplicates" = Table.Distinct(#"Marked key columns", {"nis_code", "region_fr", "region_nl", "municipality_fr", "municipality_nl", "province_fr", "province_nl"})
in
  #"Removed duplicates";
[BindToDefaultDestination = true]
shared obstacle = let
  Source = #"Traffic Accidents",
  #"Choose columns" = Table.SelectColumns(Source, {"obstacle_id", "obstacle_fr", "obstacle_nl"}),
  #"Marked key columns" = Table.AddKey(#"Choose columns", {"obstacle_id"}, false),
  #"Removed duplicates" = Table.Distinct(#"Marked key columns", {"obstacle_id", "obstacle_fr", "obstacle_nl"})
in
  #"Removed duplicates";
[BindToDefaultDestination = true]
shared road_condition = let
  Source = #"Traffic Accidents",
  #"Choose columns" = Table.SelectColumns(Source, {"road_condition_id", "road_condition_fr", "road_condition_nl"}),
  #"Marked key columns" = Table.AddKey(#"Choose columns", {"road_condition_id"}, false),
  #"Removed duplicates" = Table.Distinct(#"Marked key columns", {"road_condition_id", "road_condition_fr", "road_condition_nl"})
in
  #"Removed duplicates";
[BindToDefaultDestination = true]
shared road_type = let
  Source = #"Traffic Accidents",
  #"Choose columns" = Table.SelectColumns(Source, {"road_type_id", "road_type_fr", "road_type_nl"}),
  #"Marked key columns" = Table.AddKey(#"Choose columns", {"road_type_id"}, false),
  #"Removed duplicates" = Table.Distinct(#"Marked key columns", {"road_type_id", "road_type_fr", "road_type_nl"})
in
  #"Removed duplicates";
[BindToDefaultDestination = true]
shared road_user = let
  Source = first_road_user,
  #"Appended query" = Table.Combine({Source, second_road_user}),
  #"Marked key columns" = Table.AddKey(#"Appended query", {"road_user_id"}, false),
  #"Removed duplicates" = Table.Distinct(#"Marked key columns", {"road_user_id", "road_user_fr", "road_user_nl"})
in
  #"Removed duplicates";
[BindToDefaultDestination = true]
shared traffic_accidents = let
  Source = #"Traffic Accidents",
  #"Choose columns" = Table.SelectColumns(Source, {"year", "month", "time", "nis_code", "latitude", "longitude", "crossway_id", "weather_id", "road_condition_id", "build_up_area_id", "light_condition_id", "road_type_id", "accident_type_id", "first_road_user_id", "second_road_user_id", "collision_type_id", "obstacle_id"})
in
  #"Choose columns";
[BindToDefaultDestination = true]
shared weather = let
  Source = #"Traffic Accidents",
  #"Choose columns" = Table.SelectColumns(Source, {"weather_id", "weather_fr", "weather_nl"}),
  #"Marked key columns" = Table.AddKey(#"Choose columns", {"weather_id"}, false),
  #"Removed duplicates" = Table.Distinct(#"Marked key columns", {"weather_nl", "weather_fr", "weather_id"})
in
  #"Removed duplicates";
shared first_road_user = let
  Source = #"Traffic Accidents",
  #"Choose columns" = Table.SelectColumns(Source, {"first_road_user_id", "first_road_user_fr", "first_road_user_nl"}),
  #"Renamed columns" = Table.RenameColumns(#"Choose columns", {{"first_road_user_id", "road_user_id"}, {"first_road_user_fr", "road_user_fr"}, {"first_road_user_nl", "road_user_nl"}}),
  #"Removed duplicates" = Table.Distinct(#"Renamed columns", {"road_user_id", "road_user_fr", "road_user_nl"})
in
  #"Removed duplicates";
shared second_road_user = let
  Source = #"Traffic Accidents",
  #"Choose columns" = Table.SelectColumns(Source, {"second_road_user_id", "second_road_user_fr", "second_road_user_nl"}),
  #"Renamed columns" = Table.RenameColumns(#"Choose columns", {{"second_road_user_id", "road_user_id"}, {"second_road_user_fr", "road_user_fr"}, {"second_road_user_nl", "road_user_nl"}}),
  #"Removed duplicates" = Table.Distinct(#"Renamed columns", {"road_user_id", "road_user_fr", "road_user_nl"})
in
  #"Removed duplicates";
shared DefaultDestination = let
  Source = Lakehouse.Contents([CreateNavigationProperties = false]),
  #"Navigation 1" = Source{[workspaceId = "8f10a8d6-9370-49e5-b3c8-82dee04a1fec"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[lakehouseId = "9ad1855c-7857-469a-a449-a74b8e203ad5"]}[Data]
in
  #"Navigation 2";
