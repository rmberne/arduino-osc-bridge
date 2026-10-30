-- send_project_name.lua (Lua 5.4 / Native macOS Execution)
local HOST = "127.0.0.1"
local PORT = 9000
local last_project_name = ""

-- Construct 4-byte aligned OSC 1.0 packet string
local function pad_osc_string(str)
    local val = str .. "\0"
    local pad = (4 - (#val % 4)) % 4
    return val .. string.rep("\0", pad)
end

local function send_osc_udp(address, value)
    local osc_addr = pad_osc_string(address)
    local osc_type = ",s\0\0"
    local osc_val  = pad_osc_string(value)
    local payload  = osc_addr .. osc_type .. osc_val

    -- Convert payload bytes to double-escaped hex for zsh/perl
    local hex_payload = ""
    for i = 1, #payload do
        hex_payload = hex_payload .. string.format("\\x%02X", string.byte(payload, i))
    end

    -- Run via system zsh to bypass nc stdin buffer/close timing issues
    local cmd = string.format(
        "/bin/zsh -c 'perl -e \"print \\\"%s\\\"\" | nc -u -w0 %s %d'",
        hex_payload, HOST, PORT
    )

    reaper.ExecProcess(cmd, 0)
end

local function check_and_send_project_name()
    pcall(function()
        local proj, proj_path = reaper.EnumProjects(-1)
        local current_name = "Unsaved Project"

        if proj_path and type(proj_path) == "string" and proj_path ~= "" then
            current_name = proj_path:match("^.+[/\\](.+)%.rpp$") or proj_path:match("^.+[/\\](.+)$") or "Unsaved Project"
        end

        if current_name ~= last_project_name then
            last_project_name = current_name
            send_osc_udp("/project/name", current_name)
        end
    end)

    reaper.defer(check_and_send_project_name)
end

check_and_send_project_name()
