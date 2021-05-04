--[[

  This is a LUA CGI script that uses LibEZT to produce templated mirror content

  It uses the output from the Apache GeoIP module to choose the appropriate mirror(s)

  The script supports the following optional URL parameters:
  cca2 :
    override the country code
  preferred/Preferred :
    sets the preferred server if available, otherwise it is chosen at random
  as_json/asjson :
    don't process the template, but return the mirror data as JSON
  action=download together with filename :
    generate a redirect to the file on the preferred mirror
  archive_aware :
    sets ARCHIVE_AWARE = true ; default : false

  This script is dist, attic and archive aware.

  If the target's first path-component (TLP) has a corresponding
  file /www/attic.apache.org/projects/TLP.html,
  then the script redirects to that page.

  If the final target is not in dist, it uses template archive.html ;
  if ARCHIVE_AWARE, it looks up the target on archive.apache.org with
  a HEAD request. The lookups are cached ; forever for positive results.

]]

-- version number of this file (automatically generated by SVN)
local VERSION = ("$Revision: 1889116 $"):match("(%d+)")

function version()
    return VERSION
end

local ARCHIVE_AWARE = true -- do archive.a.o lookup with HEAD requests
local CACHE_TIMEOUT = 3600  -- should be 0 in test ; 3600 in production
local LOG_LOOKUPS   = 0     -- should be 1 in test ; 0 in production

local JSON = require 'JSON'
local ezt = require 'libezt'
local posix = require 'posix'
local SOCK = require 'socket'
local HTTP = require 'socket.http'
local HTTPS = require 'ssl.https'
-- Set 5 second max timeout for http(s) lookups
HTTP.TIMEOUT = 5
HTTPS.TIMEOUT = 5

local mirror_file = "/www/www.apache.org/mirrors/mirrors.list"
local MAXAGE = 24*3600 -- max mirror age
local ATTIC_URI = 'http://attic.apache.org/projects/'
local ATTIC_DIR = '/var/www/attic.apache.org/projects/'
local DIST_DIR  = '/var/www/www.apache.org/content/dist/'
local ARCH_URI  = 'https://archive.apache.org/dist/'
local DOWN_URI  = 'https://downloads.apache.org/'
local DYN_DIR   = '/var/www/www.apache.org/dyn/'
local CLOSER_PG  = DYN_DIR .. 'closer.html'
local ARCHIVE_PG = DYN_DIR .. 'archive.html'
local STATS_DIR  = DYN_DIR .. 'stats/'
local LOOKUP_LOG = STATS_DIR .. 'AAAA'

local dist_hit = false
local arch_hit = false
local mirror_stamp = 0 -- when mirror_file was last processed
local mirror_map = {} -- map of all recent mirrors. [ftp|http|rsync][cc|backup]=url
local mirror_map_v6 = {} -- mirror_map for ipv6-enabled mirrors
local mirror_templates = {} -- cache of unprocessed mirror templates
local mirror_templates_generated = {} -- cache of generated templates
local mymap -- map of mirrors for the current request (based on the country code


function get_mirrors()
    local now = os.time()
    local atleast = now - MAXAGE
    local f = io.open(mirror_file, "r")
    local mirrord = f:read("*a")

    -- Check the age of the mirrors relative to the mirror list, rather than now. (As was done by mirrors.cgi)
    -- This allows the system to still work even if the list is a bit stale
    -- LUA does not have a standard API to get a file date
    -- However, the timestamp when the information was collected is more useful anyway
    -- Parse the file header: # date : Wed Sep  2 09:49:53 2015 [UTC]
    local mon, day, hh, mm, ss, yy = mirrord:match("# date : %w+ (%w+) +(%d+) (%d%d):(%d%d):(%d%d) (%d%d%d%d) %[UTC%]")
    if mon then
        local MON = {Jan=1,Feb=2,Mar=3,Apr=4,May=5,Jun=6,Jul=7,Aug=8,Sep=9,Oct=10,Nov=11,Dec=12}
        -- use isdst = false as the timestamp is UTC
        local filetime = os.time({year = yy, month = MON[mon], day = day, hour = hh, min = mm, sec = ss, isdst=false})
        atleast = filetime - MAXAGE
    end

    mirror_map = {}
    mirror_map_v6 = {}
    f:close()
    for t, c, url, timestamp, ipversion in mirrord:gmatch("([a-zA-Z]+)%s+([a-zA-Z]+)%s+(%S+)%s+(%d+)(.-)\r?\n") do
        if c then
            c = c:lower()
            -- if backup, force http -> https
            if c == 'backup' then
                url = url:gsub('http://', 'https://')
            end
            -- Don't check the timestamp for backup mirrors
            if c == 'backup' or tonumber(timestamp) >= atleast then
                mirror_map[c] = mirror_map[c] or {}
                mirror_map[c][t] = mirror_map[c][t] or {}
                --url = url:gsub("/$", "")
                ipversion = ipversion:match("^%s*(%S+)$")
                if not ipversion then
                    ipversion = 'ipv4'
                end
                table.insert(mirror_map[c][t], url)
                if ipversion == 'ipv6' or c == 'backup' then
                    mirror_map_v6[c] = mirror_map_v6[c] or {}
                    mirror_map_v6[c][t] = mirror_map_v6[c][t] or {}
                    table.insert(mirror_map_v6[c][t], url)
                end
            end
        end
    end
    mirror_stamp = now
    return mirror_map
end

function log_lookup(inarch, cs, path)
    local f = io.open(LOOKUP_LOG,'a')
    if f then
        f:write(os.date('%Y-%m-%d/%H:%M:%S')
          .. " [" .. ( posix.getpid().pid or 'pid' ) .. ']'
          .. " look=" .. tostring((cs and false or true))
          .. " hit="  .. tostring( inarch )
          .. ' ' .. path
          .. "\n"
          )
        f:close()
    end
end

function interval(t)       return 1000 * ( SOCK.gettime() - t ) end
function elapsed(t)        return string.format("%.3f ms'\n",interval(t)) end
function file_exists(file) return posix.stat(file) ~= nil end
function is_in_attic(proj) return file_exists(ATTIC_DIR .. proj .. '.html') end
function arch_uri(path)    return ARCH_URI .. path end
function dl_uri(path)      return DOWN_URI .. path end

function archive_url(path)
    local uri = arch_uri(path)
    return '<a href="' .. uri .. '" rel="nofollow">' .. uri .. '</a>'
end

function is_in_arch(r, path)
    -- Cached lookups on archive.a.o for files and dirs. a non-404 response code is considered a hit
    local cache_hit_result = r:ivm_get("archive_ao_cache_result_" .. path)
    local cache_hit_stamp = r:ivm_get("archive_ao_cache_stamp_" .. path)
    local exists = false
    local is_fresh = false
    if cache_hit_stamp then
        is_fresh = ( os.time() - cache_hit_stamp ) < CACHE_TIMEOUT
        if is_fresh then
            exists = (cache_hit_result == 1)
        end
    end
    if not is_fresh
    then
        local rv, c, h, _ = HTTPS.request { method = "HEAD", url = arch_uri(path), sink = ltn12.sink.table(resp), protocol = "tlsv1_2" }
        exists = ( c and c ~= 404 )
        r:ivm_set("archive_ao_cache_result_" .. path, exists and 1 or 0)
        r:ivm_set("archive_ao_cache_stamp_" .. path, os.time())
    end
    return exists, cache_hit_stamp
end

function is_on_downloads_ao(r, path)
    -- Cached lookups on downloads.a.o for files and dirs. a non-404 response code is considered a hit
    local cache_hit_result = r:ivm_get("downloads_ao_cache_result_" .. path)
    local cache_hit_stamp = r:ivm_get("downloads_ao_cache_stamp_" .. path)
    local exists = false
    local is_fresh = false
    if cache_hit_stamp then
        is_fresh = ( os.time() - cache_hit_stamp ) < CACHE_TIMEOUT
        if is_fresh then
            exists = (cache_hit_result == 1)
        end
    end
    if not is_fresh
    then
        local rv, c, h, _ = HTTPS.request { method = "HEAD", url = dl_uri(path), sink = ltn12.sink.table(resp), protocol = "tlsv1_2" }
        exists = ( c and c ~= 404 )
        r:ivm_set("downloads_ao_cache_result_" .. path, exists and 1 or 0)
        r:ivm_set("downloads_ao_cache_stamp_" .. path, os.time())
    end
    return exists, cache_hit_stamp
end

function get_page(url)
    if not mirror_templates[url] or mirror_templates[url].timestamp < (os.time() - 2*CACHE_TIMEOUT) then
        local f = io.open(url, "r")
        mirror_templates[url] = {
            data = f and f:read("*a") or "No such page",
            timestamp = os.time()
        }
        if f then
            f:close()
        end
    end
    return mirror_templates[url]
end

function get_output_cached(page, defs, r, ezt_defs)
    local pref = defs.preferred or ""
    local path_info = defs.path_info or ""
    local cacheKey = page .. ":" .. pref .. ":" .. path_info 
    if not mirror_templates_generated[cacheKey] or mirror_templates_generated[cacheKey].timestamp < (os.time() - CACHE_TIMEOUT) then
        local template = get_page(page)
        local tdata = recurse(defs, template.data, r, ezt_defs)
        mirror_templates_generated[cacheKey] = {
            data = tdata,
            timestamp = os.time()
        }
    end
    return mirror_templates_generated[cacheKey]
end

function recurse(defs, tdata, r, ezt_defs)
    -- SSI emulation
    tdata = tdata:gsub("<!%-%-%s*#include virtual=\"(.-)\"%s*%-%->",
        function(inc)
            local filepath = (defs.filepath .. inc):gsub("[/]+", "/")
            if r:stat(filepath) then
                local f = io.open(filepath, "r")
                local d = f:read("*a")
                f:close()
                return d
            else
                return ""
            end
        end
    )
    
    -- Parse EZT
    local structure, error = ezt:import("[ezt]"..tdata.."[end]")
    
    -- Render output
    if structure then return ezt:construct(structure, ezt_defs) else return error end
end

-- true if the string (s) ends with (e)
function endsWith(s, e)
    return e == s:sub(-e:len())
end

-- true if the string (s) begins with (b)
function beginsWith(s, b)
    return b == s:sub(1, b:len())
end

-- return false if string is empty (or nil)
function nonEmpty(s)
    if s == null or s == '' then return nil end
    return s
end

-- Temporary fix to extract the missing path_info for dyn/closer.cgi redirects only
function get_path_info(s)
    local CGI_SCRIPT = "/dyn/closer.cgi/" -- original CGI script name
    if beginsWith(s, CGI_SCRIPT) then
        return s:sub(CGI_SCRIPT:len()) -- keep just the suffix
    else
        return nil
    end
end

-- The request parameter has the data structures and functions as described here:
-- http://httpd.apache.org/docs/trunk/mod/mod_lua.html#datastructures
-- http://httpd.apache.org/docs/trunk/mod/mod_lua.html#functions

function handle(r)
    r.headers_out['Cache-Control'] = 'private' -- Invalidate any cache
    local get = r:parseargs()
    if get.archive_aware and not ( get.archive_aware == '0' ) then
      ARCHIVE_AWARE = true
    end
    
    local now = os.time()
    if mirror_stamp < (now - 3600) then
        get_mirrors()
    end
    local country = r.notes['GEOIP_COUNTRY_NAME'] or r.subprocess_env['GEOIP_COUNTRY_NAME'] or "Unknown"
    local cca2 = (get.cca2 or r.notes['GEOIP_COUNTRY_CODE'] or r.subprocess_env['GEOIP_COUNTRY_CODE'] or r.subprocess_env['GEOIP_COUNTRY_CODE_V6'] or 'Backup'):lower()
    if cca2 == 'gb' then
        cca2 = 'uk'
    end
    local client_is_ipv6 = r.useragent_ip:match("(:[a-f0-9]+):?:?$") and true or false
    local occa2 = cca2
    if not mirror_map[cca2] then
        cca2 = 'backup'
    end
    mymap = mirror_map[cca2] or mirror_map['backup']
    if client_is_ipv6 then
        mymap = mirror_map_v6[cca2] or mirror_map['backup']
    end
    local bmap = mirror_map['backup']
    mymap['backup'] = bmap['http']
    local URL = {}
    for _, t in pairs({'http','ftp'}) do
        URL[t] = (mymap[t] and mymap[t][math.random(1, #mymap[t])]) or (bmap[t] and bmap[t][math.random(1, #bmap[t])])
    end
    local page = r.filename
    local got_f = get.f -- work on a copy of the parameter
    if got_f then
        -- path normalization: We get all sorts of /var/www, /www/ (or nothing!) etc thrown at us,
        -- due to legacy puppet cruft and EU/US alternate hostnames. We want to normalize that.
        local hname = r.hostname:gsub("www%.", "")
        got_f = got_f:gsub("^/var/www/html/", "/var/www/")
        got_f = got_f:gsub(hname, ""):gsub("/var/www//var/www/", "/var/www/")
        got_f = got_f:gsub("^/var/www//?www/", "/var/www/")
        if r:stat(got_f) or r:stat(got_f:gsub("%.cgi", ".html"))  then
            page = got_f
        else
            page = got_f:gsub("/www/", "/www/" .. hname:gsub("%.[eu][us]%.", ".") .. "/"):gsub("[/]+", "/")
        end
    end
    -- Rewrite foo.cgi or foo.lua to foo.html
    page = page:gsub("%.cgi", ".html"):gsub("%.lua", ".html")
    -- Ensure the target template exists, or fall back to default template
    if not r:stat(page) or not (page:match("^/var/www/") or page:match("^/www/")) then
        page = CLOSER_PG
    end
    -- Final sanity check: page variable must match this path to be a valid template file
    -- If not, default to our standard template
    -- TODO: Weed out the /var/www later on and always only have /www/foo.a.o/bar.html as valid
    -- Do not allow '.' in path segments apart from the last (the file name)
    if not r:regex(page, [[^(/var/www|/www)/([-a-z0-9]+\.apache\.org)/([-_a-zA-Z0-9/]+/)?[-_a-zA-Z0-9.]+\.html?$]]) then
        page = CLOSER_PG
    end
    
    local defs = {}
    local ezt_defs = {
        strings = {},
        arrays = {}
    }
    
    defs.filepath = page:gsub("[^/]+$", "")
    defs.debug = get.debug and true or false
    defs.preferred = r:escape_html(get.preferred or get.Preferred or URL['http'] or "")
    defs.path_info = r:escape_html(get.path -- command-line override
         or nonEmpty(r.path_info) -- if path provided by server
         or get_path_info(r.uri) -- temporary fix to extract it from r.uri for dyn/closer.cgi calls
         -- Disable for now; it was previously effectively disabled because r.path_info was never false
--         or r.unparsed_uri:gsub("^.+%.cgi/*", ""):gsub("^.+%.lua/*", "") -- not sure what this is trying to do
         -- TODO in any case seems wrong to use the unparsed URI as that will include the query string
         or "/") -- default
        :gsub("^/","",1) -- trim leading "/" as per Python version
    defs.country = country
    defs.cca2 = cca2
    defs.ipv6 = client_is_ipv6
    -- proj is the first path component of defs.path_info
    local proj = defs.path_info
    if proj and proj:find('/') then
      proj = proj:sub(1,proj:find('/')-1)
    end
    defs.project = proj
    ezt_defs.strings = defs
    ezt_defs.arrays = {
        http = mymap['http'] or bmap['http'],
        ftp = mymap['ftp'] or bmap['ftp'],
        backup = bmap['http'],
    }
    -- Check that preferred http/ftp exists, otherwise default to none
    local prefIsOkay = false
    for _,b in ipairs({'http', 'ftp', 'backup'}) do 
        for _, v in pairs(ezt_defs.arrays[b] or {}) do -- arrays[b] may not exist
            if r:escape_html(v) == defs.preferred then
                prefIsOkay = true
                break
            end
        end
        if prefIsOkay then
            break
        end
    end
    if not prefIsOkay then
        ezt_defs.preferred = ""
        defs.preferred = URL['http']
    end
    
    -- string only repr of preferred URL
    if get.preferred and get.preferred == "true" then
        r.content_type = "text/plain"
        r:puts(defs.preferred)
        return apache2.OK
    end
    
    local do_json = false
    if (get.as_json and not (get.as_json == "0")) or (get.asjson and not (get.asjson == "0")) then
        do_json = true
    end
    if get.action then
        local d_uri = get.filename or nonEmpty(defs.path_info)
        if get.action == 'download' and nonEmpty(d_uri) then
            if is_on_downloads_ao(r, d_uri) then
                r.headers_out['Location'] = defs.preferred .. d_uri
                r.status = 302
                return apache2.OK
            elseif is_in_arch(r, d_uri) then
                r.headers_out['Location'] = ARCH_URI .. d_uri
                r.status = 302
                return apache2.OK
            else
                r.content_type = "text/plain"
                r.status = 404
                r:puts("The requested file does not exist in our mirror system or in our archives.")
                return apache2.OK
            end
            
        elseif get.action == 'info' then
            r.content_type = "text/plain"
            r:puts(string.format("%s\ncloser revision: %s\nlibezt revision: %s\n",
                 _VERSION, -- LUA 
                 version(), -- closer
                  ezt:version())) -- libezt
            -- Show any arguments
            for k, v in pairs( get ) do
                r:puts( string.format("arg %s: %s\n", k, v) )
            end
            local t0 = SOCK.gettime() ;
            local URI = r.subprocess_env['SCRIPT_URI'] or "nil"
            -- Request parameters
            r:puts("r.hostname               : '",r.hostname or "nil", "'\n")
            r:puts("r.document_root          : '",r.document_root or "nil", "'\n")
            r:puts("r.uri                    : '",r.uri or "nil", "'\n")
            -- r:puts("r.the_request:  '",r.the_request or "nil", "'\n")
            -- r:puts("r.unparsed_uri: '",r.unparsed_uri or "nil", "'\n")
            r:puts("r.path_info              : '",r.path_info or "nil","'\n")
            r:puts("env[SCRIPT_URI]          : '",URI,"'\n")
            -- calculated values
            r:puts("defs.path_info           : '",defs.path_info or "nil","'\n")
            r:puts("defs.filepath            : '",defs.filepath or "nil","'\n")
            r:puts("occa2                    : '",occa2,"'\n")
            r:puts("proj                     : '",proj,"'\n")
            r:puts("proj in attic            : '",tostring(is_in_attic(proj)),"'\n")
            r:puts("elapsed                  : '",elapsed(t0))
            r:puts("... dist lookup ...\n")
            local on_downloads_ao, cs = is_on_downloads_ao(r, defs.path_info)
            r:puts("exists on downloads.a.o? : '",tostring(on_downloads_ao), "'\n")
            r:puts("cache stamp              : '",tostring(cs), "'\n")
            r:puts("dist uri                 : '",dl_uri(defs.path_info),"'\n")
            r:puts("elapsed                  : '",elapsed(t0))
            r:puts("archive aware            : '",tostring(ARCHIVE_AWARE),"'\n")
            if on_downloads_ao == 'false' then
              r:puts("archive uri              : '",arch_uri(defs.path_info),"'\n")
            end
            if ARCHIVE_AWARE then
              r:puts("... archive lookup ...\n")
              r:puts("process PID              : '",tostring(posix.getpid().pid),"'\n")
              local in_arch, cs = is_in_arch(r, defs.path_info)
              r:puts("archive uri              : '",arch_uri(defs.path_info),"'\n")
              r:puts("path in arch?            : '",tostring(in_arch),"'\n")
              r:puts("arch stamp               : '",tostring(cs),"'\n")
              r:puts("elapsed                  : '",elapsed(t0))
            end
            return apache2.OK
        elseif get.action == 'catlog' then
            r.content_type = "text/plain"
            local f = io.open(LOOKUP_LOG)
            if f then
              while true do
                local line = f:read()
                if line == nil then break end
                r:puts(line,"\n")
              end
              f:close()
            else
              r:puts("can't open " .. LOOKUP_LOG .. "\n")
            end
            return apache2.OK
        else
            r.content_type = "text/plain"
            r:puts("unknown action [" .. get.action .. "]\n")
            return apache2.OK
        end
    end
    if do_json then
        r.content_type = "application/json"
        r:puts(JSON:encode_pretty({
            path_info = defs.path_info,
            preferred = defs.preferred,
            http = mymap['http'] or bmap['http'],
            ftp = mymap['ftp'] or bmap['ftp'],
            backup = bmap['http'],
            ipv6 = defs.ipv6,
            in_dist  = is_on_downloads_ao(r, defs.path_info),
            in_attic = is_in_attic(proj),
            cca2 = occa2
        }))
        return apache2.OK
    end
    if is_in_attic(proj) then
        r.headers_out['Location'] = ATTIC_URI .. proj .. ".html"
        r.status = 302
        return apache2.OK
    end
    if not is_on_downloads_ao(r, defs.path_info) then
        local arch_home = archive_url('') ;
        local arch_path = archive_url(defs.path_info)
        local lookup = '' ;
        if ARCHIVE_AWARE
        then
          local inarch, cs = is_in_arch(r, defs.path_info)
          if inarch == nil then
            if HTTP then
              lookup = 'A lookup on ' .. arch_home .. ' failed.'
            else
              lookup = "Can't do lookups on " .. arch_home
            end
            lookup = lookup .. "<br>Try " .. arch_path
          elseif inarch then
            lookup = 'The object is in our archive : ' .. arch_path
          else
            lookup = 'The object is in not in our archive ' .. arch_home
          end
          if LOG_LOOKUPS then log_lookup(inarch, cs, defs.path_info) end
        else -- not ARCHIVE_AWARE
            lookup = "It may be in our archive : " .. arch_path
        end
        defs.lookup = lookup
        page = ARCHIVE_PG
    end
    local rootpath = defs.path_info:match("^([-a-z0-9]+)/")
    if rootpath and rootpath == "incubator" then
        rootpath = defs.path_info:match("^incubator/([-a-z0-9]+)/")
    end
    if rootpath then
        local f = io.open(STATS_DIR .. rootpath .. ".log", "a")
        if f then
            -- get a bit of the IP to identify multiple unique request with same TS/CCA2
            local ipbit = r.useragent_ip:match("([a-f0-9]+):?:?$") or r.useragent_ip:match("^([a-f0-9]+)") or "000"
            f:write(os.time() .. " " .. ipbit .. " " .. occa2 .. " " .. defs.path_info .. "\n")
            f:close()
        end
    end
    local tdata = get_output_cached(page, defs, r, ezt_defs)

    -- check for special content-type based on file name
    if endsWith(page,"--xml.html") then
        r.content_type = "text/xml"
    else
        r.content_type = "text/html"
    end
    r:puts(tdata.data)
    if r.hostname == 'www.apache.org' then
      r:puts("<!-- " .. occa2 .. " -->")
    end
    return apache2.OK
end
