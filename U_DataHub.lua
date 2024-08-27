-- Custom device panel and control for MetOffice_DataHub device type
-- ...but note that the filename is just "U_DataHub.lua"

local xml = require "openLuup.xml"
local api = require "openLuup.api"

local h = xml.createHTMLDocument ()    -- for factory methods
local span, div = h.span, h.div
local img = h.img
local a = h.a
local br = h.br()

local Weather_types = {
  ["NA"] = "Not available",
  [0] = "Clear night",
  "Sunny day", "Partly cloudy (night)", "Partly cloudy (day)", "Not used", "Mist",                            -- 1–5
  "Fog", "Cloudy", "Overcast", "Light rain shower (night)", "Light rain shower (day)",                        -- 6–10
  "Drizzle", "Light rain", "Heavy rain shower (night)", "Heavy rain shower (day)", "Heavy rain",              -- 11–15
  "Sleet shower (night)", "Sleet shower (day)", "Sleet", "Hail shower (night)", "Hail shower (day)",          -- 16-20
  "Hail", "Light snow shower (night)", "Light snow shower (day)", "Light snow", "Heavy snow shower (night)",  -- 21–25
  "Heavy snow shower (day)", "Heavy snow", "Thunder shower (night)", "Thunder shower (day)", "Thunder",       -- 26–30 
}
 
local tendency = {F = "falling", R = "rising", S = "steady"}

local conditions = {
  "%s, pressure %s mb.",
  "Wind %sº %.0f mph gusting to %.0f mph.",
  "Visibility %s m. Dew point %sº."}

local MetLogo = 
  "https://www.metoffice.gov.uk/binaries/content/gallery/metofficegovuk/images/about-us/website/mo_master_for_dark_backg_rbg.png"
local DPabout = "https://www.metoffice.gov.uk/services/data/met-office-weather-datahub"
local DPdocs  = "https://datahub.metoffice.gov.uk/docs/getting-started"
local DPstations = "https://www.metoffice.gov.uk/research/climate/maps-and-data/uk-synoptic-and-climate-stations"

return {
  panel = function(devNo)
    local D = api[devNo]
    local S = D.properties
    local dataDate = S.modelRunDate or "--/--/--"
    local T = D.temp.CurrentTemperature or '?'
    local H = D.humid.CurrentLevel or '?'

    return div {
        span {class="w3-large w3-text-dark-gray", T, "°, ", H, '%', br, ''},
        div {class = "w3-tiny w3-display-bottomright", dataDate}
      }
  end,

  control = function(devNo)
    local D = api[devNo]
    local S = D.latest
    local W = S.significantWeatherCode or '?'
    W = Weather_types[tonumber(W) or W] or '?'
    local P = S.mslp / 100
    local B,X,G = S.windDirectionFrom10m, S.windSpeed10m * 2.2369, S.windGustSpeed10m * 2.2369
    local V = S.visibility
    local Dp = S.screenDewPointTemperature
    
    local line0 = h.strong {D.properties.LocationName .. ':'}
    local line1 = conditions[1]: format (W, P)
    local line2 = conditions[2]: format(B, X, G)
    local line3 = conditions[3]: format(V, Dp)
    
    return 
      div {
        div {class="w3-gray w3-round-large", img {alt="Met Office logo", width=400, src= MetLogo}},
        div {class="w3-section",
          div {class="w3-gray w3-text-white w3-large w3-text-white w3-round-large w3-cell",
            span {class="w3-margin-left w3-x-large", "DataHub"},
            div {class="w3-medium",
              a {class = "w3-round w3-lime w3-button w3-margin-left w3-margin-top", href=DPabout, target="_blank", "About"},
              a {class = "w3-round w3-lime w3-button w3-margin-left w3-margin-top", href=DPdocs, target="_blank", "Documentation"},
              a {class = "w3-round w3-lime w3-button w3-margin-left w3-margin-right w3-margin-top", href=DPstations, target="_blank", "Stations"},
              div {class="w3-light-blue w3-round-large w3-padding w3-cell-middle w3-margin", line0, br, line1, br, line2, br, line3},    
              },
            },
          },
      }
    end,
 
}
