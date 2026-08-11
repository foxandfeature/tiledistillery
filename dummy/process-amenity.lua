-- Second minimal dummy tilemaker profile, used alongside process.lua only
-- to exercise this repo's own CI building two output_basenames together
-- in one _pipeline.yml call (comma-separated config/process/output_basename,
-- one shared download per region — see docs/ARCHITECTURE.md
-- "Distribution"). Not an example to copy for a real layer.

function node_function(node)
	local amenity = Find("amenity")
	if amenity ~= "" then
		Layer("amenity")
		Attribute("class", amenity)
		Attribute("name", Find("name"))
	end
end

function way_function()
	-- no way-derived layers in this dummy profile
end
