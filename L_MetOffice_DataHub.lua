module(..., package.seeall)

_G.ABOUT = {
  NAME          = "L_MetOffice_DataHub",
  VERSION       = "2024.08.24",
  DESCRIPTION   = "WeatherApp using MetOffice Weather Hub",
  AUTHOR        = "@akbooer",
  COPYRIGHT     = "(c) 2022-present AKBooer",
  DOCUMENTATION = "",
  DEBUG         = false,
  LICENSE       = [[
  Copyright 2022-present AK Booer

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
]]
}

-- 2022.11.04  original version
-- 2022.11.06  make child devices optional, using 'children' device attribute containing T and/or H
-- 2022.11.08  fix for data split across two day intervals 
-- 2022.11.14  change ServiceId separator to ':' from '.' (less confusing)

-- 2024.07.22  add MaxTemp and MinTemp (handled by Data Historian rules)
-- 2024.07.28  add more checks for returned data structure (including JSON global)

-- 2024-08-24  derived from DataPoint app, modified to use new API

--[[
see:
  https://www.metoffice.gov.uk/services/data/met-office-weather-datahub
  https://datahub.metoffice.gov.uk/docs/f/category/site-specific/type/site-specific/api-documentation#get-/point/hourly  
also:
  https://github.com/MetOffice/weather_datahub_utilities
--]]

local json    = require "openLuup.json"
local tables  = require "openLuup.servertables"     -- for standard DEV and SID definitions
local API     = require "openLuup.api"              -- new openLuup API
local https   = require "ssl.https"
local ltn12   = require "ltn12"

local luup = _G.luup
--local ABOUT = _G.ABOUT

local DEV = tables.DEV

local _log = luup.log

-----

local url = "https://data.hub.api.metoffice.gov.uk/sitespecific/v0/point/hourly?"

local empty = {}

local zero_dp = "%.0f"
local one_dp  = "%.1f"

local function update_readings (p)

  local D = API[p.D]      -- this device
  local key = D.attr.key 
  
  local params = {
      excludeParameterMetadata = true,
      includeLocationName = true,
      latitude = p.lat,
      longitude = p.long,
    }
    
  local req = {}
  for name,value in pairs (params) do 
  --  req[#req+1] = table.concat {name, '=', URL.escape(tostring(value))}
    req[#req+1] = table.concat {name, '=', (tostring(value))}
  end
  local url2 = url .. table.concat(req, '&')
  
  local result = {}
  local _,code = https.request {
      url = url2, 
      headers = {apikey = key},
      sink = ltn12.sink.table(result)
    }		-- body, code, headers, status

  local Json
  result = table.concat(result)
  _G.JSON = result       -- 2024.07.28  save in global for debug purposes
  
  if code ~= 200 then
    _log ("error polling MetOffice DataHub, return code = " .. (code or 'nil'))
    Json = {}
  else
    Json = json.decode (result)
  end
 
  
  local x = Json
  local properties = x.features and x.features[1] and x.features[1].properties
  if not properties then
    _log ("error Features collection missing: " .. result)
    return
  end
  
  local location = (properties.location or empty)
  local modelRunDate = properties.modelRunDate
  local timeseries = properties.timeSeries or empty
  
  D.properties.LocationName = location.name
  D.properties.modelRunDate = modelRunDate
  
  local latest = (timeseries or empty) [1] or empty
  
  local S = D.latest
  for n,v in pairs(latest) do -- latest time series
    S[n] = v
  end
  
  do -- update parent and child standard device variables
    local t = one_dp: format(latest.screenTemperature)
    D.temp.CurrentTemperature = t
    D.temp.MaxTemp = t               -- 2024.07.22
    D.temp.MinTemp = t               -- Max/Min handled by Historian rules
    
    local h = zero_dp: format(latest.screenRelativeHumidity)
    D.humid.CurrentLevel = h
    
    D.generic.Pressure = zero_dp: format(latest.mslp / 100)   -- convert Pascal to millibars
    
    if p.T then
      API[p.T].temp.CurrentTemperature = t
    end
    
    if p.H then
      API[p.H].humid.CurrentLevel = h
    end
  end

--]]

  D.hadevice.LastUpdate = os.time()
  _log ("MetOffice DataHub: " .. latest.time)
  
end

local function poll (p)
  update_readings (p)

  -- rechedule 
  API.timers "delay" {
    callback = poll, 
    delay = 10 * 60,      -- ten minutes
    parameter = p, 
    name = "DataHub polling"}
end

function init (lul_device)
  local devNo = tonumber (lul_device)
  local A = API[devNo].attr
  
  local T, H
  local lat, long
  do -- create essential attributes if they don't exist
    A.key = A.key or "API key?  "
    A.children = A.children or "T and H"
    A.latitude = A.latitude or luup.latitude
    A.longitude = A.longitude or luup.longitude
    
    T = A.children: match "T"     -- non-nil if child to be created
    H = A.children: match "H"     -- ditto
    
    lat = A.latitude
    long = A.longitude
  end
    
  local dev_t, dev_h
  do -- create children
    local children = luup.chdev.start(devNo)
    -- use non-standard device number return parameter (openLuup only) for chdev.append()
    if T then
      dev_t = luup.chdev.append (devNo, children, "MetT", "Met Temperature", '', DEV.temperature, '', '', false)
    end
    if H then
      dev_h = luup.chdev.append (devNo, children, "MetH", "Met Humidity",    '', DEV.humidity,    '', '', false)
    end
    luup.chdev.sync(devNo, children)
  end
  
  do -- delay polling startup
    API.timers "delay" {
      callback = poll, 
      delay = 10,       -- ten seconds
      parameter = {D = devNo, T = dev_t, H = dev_h, lat = lat, long = long}, 
      name = "DataHub delayed startup"}
  end

  luup.set_failure (0)
  return true, "OK", "MetOffice_DataHub"
end

-----
