-- Minimal dummy tilemaker profile used only by this repo's own CI
-- self-test (see docs/ARCHITECTURE.md "CI self-test"). Emits one "place"
-- layer from place=* nodes (cities, towns, villages, ...) — enough to
-- prove config-referencing, sharding, claiming, and merge all work end to
-- end against a real Geofabrik extract, with no domain-specific logic.
-- Real consumers (e.g. the future trashtracker-tiles repo) bring their own
-- config.json/process.lua; this one is deliberately not an example to copy.

function node_function(node)
	local place = Find("place")
	if place ~= "" then
		Layer("place")
		Attribute("class", place)
		Attribute("name", Find("name"))
	end
end

function way_function()
	-- no way-derived layers in this dummy profile
end
