<#
Keyboard capture script for Windows (PowerShell 5+, Win10/11).
Captures global key events via a low-level keyboard hook (user32.dll) and logs
them to a JSONL file plus console.

Run from an elevated PowerShell if required by security policy:
  powershell -ExecutionPolicy Bypass -File .\keyboard_capture.ps1 -LogPath .\keyboard_log.jsonl

Outputs JSONL rows like:
{"ts":"2025-12-16T09:00:00.0000000Z","vk":162,"scan":29,"name":"ControlKey","flags":0,"msg":256}
Where msg: 256=WM_KEYDOWN, 257=WM_KEYUP, 260=WM_SYSKEYDOWN, 261=WM_SYSKEYUP
flags: bit 0x01=extended key, 0x10=injected, see KBDLLHOOKSTRUCT.
#>

[CmdletBinding()]
param(
    # Default log in %TEMP% to avoid path issues in MDT/fresh installs.
    [string]$LogPath = "$env:TEMP\\workflow\\keyboard\\keyboard_log.jsonl",
    # Folder for configs (e.g., network share \\mdt.local\\conf\\keyboard); defaults to local "keyboard_conf".
    [string]$ConfigDir = "$PSScriptRoot\\keyboard_conf",
    # Config file name (will be created inside ConfigDir if relative)
    [string]$ConfigPath = "",
    # Keyboard layout: fr-azerty (default) or us-qwerty; or use a custom JSON via LayoutConfig.
    [string]$Layout = "fr-azerty",
    # Optional JSON file defining layoutRows, scanMap, labels, widthOverrides.
    [string]$LayoutConfig = "",
    # If set, swallow keyboard events so they ne se propagent pas aux autres apps.
    [switch]$BlockInput,
    # Show the PowerShell console (hidden by default).
    [switch]$ShowConsole
)

Add-Type -AssemblyName System.Windows.Forms

$nativeConsole = @"
using System;
using System.Runtime.InteropServices;
public static class ConsoleWindow {
    [DllImport("kernel32.dll")]
    public static extern IntPtr GetConsoleWindow();
    [DllImport("user32.dll")]
    public static extern bool ShowWindow(IntPtr hWnd, int nCmdShow);
}
"@

try {
    Add-Type -TypeDefinition $nativeConsole -ErrorAction Stop
    if (-not $ShowConsole) {
        $consoleHandle = [ConsoleWindow]::GetConsoleWindow()
        if ($consoleHandle -ne [IntPtr]::Zero) {
            [ConsoleWindow]::ShowWindow($consoleHandle, 0) | Out-Null
        }
    }
} catch {
    # Ignore console hide issues; continue with UI.
}

$csharp = @"
using System;
using System.ComponentModel;
using System.Runtime.InteropServices;
using System.Threading;

namespace WinAPI {
    public static class GlobalKeyboardHook {
        // Return true from the delegate to consume (block) the key.
        public delegate bool KeyboardEvent(int vkCode, int scanCode, int flags, int msg);
        public static event KeyboardEvent KeyEvent;

        private const int WH_KEYBOARD_LL = 13;
        private static IntPtr _hookId = IntPtr.Zero;
        private static HookProc _hookProc = HookCallback;
        private static Thread _messageThread;
        private static int _threadId = 0;
        private static bool _stop = false;

        private delegate IntPtr HookProc(int nCode, IntPtr wParam, IntPtr lParam);

        [StructLayout(LayoutKind.Sequential)]
        private struct KBDLLHOOKSTRUCT {
            public uint vkCode;
            public uint scanCode;
            public uint flags;
            public uint time;
            public IntPtr dwExtraInfo;
        }

        [StructLayout(LayoutKind.Sequential)]
        private struct POINT {
            public int x;
            public int y;
        }

        [StructLayout(LayoutKind.Sequential)]
        private struct MSG {
            public IntPtr hwnd;
            public uint message;
            public IntPtr wParam;
            public IntPtr lParam;
            public uint time;
            public POINT pt;
        }

        public static void Start() {
            if (_hookId != IntPtr.Zero) return;

            _hookId = SetWindowsHookEx(WH_KEYBOARD_LL, _hookProc, GetModuleHandle(null), 0);
            if (_hookId == IntPtr.Zero) {
                throw new Win32Exception(Marshal.GetLastWin32Error());
            }

            _stop = false;
            _messageThread = new Thread(MessageLoop);
            _messageThread.IsBackground = true;
            _messageThread.Start();
        }

        public static void Stop() {
            _stop = true;
            if (_threadId != 0) {
                PostThreadMessage((uint)_threadId, 0x0012 /* WM_QUIT */, IntPtr.Zero, IntPtr.Zero);
            }
            if (_hookId != IntPtr.Zero) {
                UnhookWindowsHookEx(_hookId);
                _hookId = IntPtr.Zero;
            }
            _threadId = 0;
        }

        private static void MessageLoop() {
            _threadId = GetCurrentThreadId();
            MSG msg;
            while (!_stop && GetMessage(out msg, IntPtr.Zero, 0, 0) != 0) {
                // no-op; loop keeps hook alive
            }
        }

        private static IntPtr HookCallback(int nCode, IntPtr wParam, IntPtr lParam) {
            if (nCode >= 0) {
                var info = (KBDLLHOOKSTRUCT)Marshal.PtrToStructure(lParam, typeof(KBDLLHOOKSTRUCT));
                var handler = KeyEvent;
                if (handler != null) {
                    var consume = handler((int)info.vkCode, (int)info.scanCode, (int)info.flags, (int)wParam);
                    if (consume) {
                        return (IntPtr)1;
                    }
                }
            }
            return CallNextHookEx(_hookId, nCode, wParam, lParam);
        }

        [DllImport("user32.dll", SetLastError = true)]
        private static extern IntPtr SetWindowsHookEx(int idHook, HookProc lpfn, IntPtr hMod, uint dwThreadId);

        [DllImport("user32.dll", SetLastError = true)]
        [return: MarshalAs(UnmanagedType.Bool)]
        private static extern bool UnhookWindowsHookEx(IntPtr hhk);

        [DllImport("user32.dll", SetLastError = true)]
        private static extern IntPtr CallNextHookEx(IntPtr hhk, int nCode, IntPtr wParam, IntPtr lParam);

        [DllImport("kernel32.dll", CharSet = CharSet.Auto, SetLastError = true)]
        private static extern IntPtr GetModuleHandle(string lpModuleName);

        [DllImport("user32.dll")]
        private static extern int GetMessage(out MSG lpMsg, IntPtr hWnd, uint wMsgFilterMin, uint wMsgFilterMax);

        [DllImport("user32.dll")]
        private static extern bool PostThreadMessage(uint idThread, uint Msg, IntPtr wParam, IntPtr lParam);

        [DllImport("kernel32.dll")]
        private static extern int GetCurrentThreadId();
    }
}
"@

Add-Type -TypeDefinition $csharp -Language CSharp

$queue = New-Object System.Collections.Concurrent.BlockingCollection[pscustomobject]
# Track unique keys for config generation.
$seenKeys = [System.Collections.Generic.HashSet[string]]::new()
$keyInfo = @{}
$script:completionWritten = $false
$script:finalStatus = 'not_tested'
$script:padStatus = 'not_tested'
$script:logStream = $null
$script:labelOverrides = @{}
$script:blockInputs = $BlockInput.IsPresent
$script:minOkRatio = 0.5

function Write-CompletionStatus {
    param(
        [string]$Status,
        [string[]]$Missing = @()
    )
    if ($Status) {
        $script:finalStatus = $Status.ToUpperInvariant()
    }
    $payload = [pscustomobject]@{
        ts      = (Get-Date).ToUniversalTime().ToString("o")
        status  = $Status
        missing = $Missing
        padStatus = $script:padStatus
    }
    $line = $payload | ConvertTo-Json -Compress
    if ($script:logStream) {
        $script:logStream.WriteLine($line)
        $script:logStream.Flush()
    } else {
        Write-Warning "Log stream not ready; status '$Status' not written to file."
    }
    $script:completionWritten = $true
}

# Build a simple visual keyboard that lights keys when pressed.
function Get-CommonWidthOverrides {
    return @{
        'Back'        = 90
        'Tab'         = 70
        'CapsLock'    = 90
        'Return'      = 95
        'LShiftKey'   = 110
        'RShiftKey'   = 130
        'Space'       = 430
        'LControlKey' = 80
        'RControlKey' = 80
        'LMenu'       = 80
        'RMenu'       = 80
        'LWin'        = 80
        'RWin'        = 80
        'Apps'        = 80
        'NumLock'     = 70
        'Divide'      = 60
        'Multiply'    = 60
        'Subtract'    = 70
        'Add'         = 70
        'Decimal'     = 60
        'NumPad0'     = 120
    }
}

function Get-FrScanKeyMap {
    $map = @{}
    # Top row (AZERTY FR)
    $map[41] = 'Oemtilde'      # ²
    $map[2]  = 'D1'
    $map[3]  = 'D2'
    $map[4]  = 'D3'
    $map[5]  = 'D4'
    $map[6]  = 'D5'
    $map[7]  = 'D6'
    $map[8]  = 'D7'
    $map[9]  = 'D8'
    $map[10] = 'D9'
    $map[11] = 'D0'
    $map[12] = 'OemMinus'
    $map[13] = 'Oemplus'
    $map[14] = 'Back'
    # Second row
    $map[15] = 'Tab'
    $map[16] = 'A'
    $map[17] = 'Z'
    $map[18] = 'E'
    $map[19] = 'R'
    $map[20] = 'T'
    $map[21] = 'Y'
    $map[22] = 'U'
    $map[23] = 'I'
    $map[24] = 'O'
    $map[25] = 'P'
    $map[26] = 'OemOpenBrackets' # ^ ¨
    $map[27] = 'Oem6'            # $ £
    $map[28] = 'Return'
    # Third row
    $map[58] = 'CapsLock'
    $map[30] = 'Q'
    $map[31] = 'S'
    $map[32] = 'D'
    $map[33] = 'F'
    $map[34] = 'G'
    $map[35] = 'H'
    $map[36] = 'J'
    $map[37] = 'K'
    $map[38] = 'L'
    $map[39] = 'M'
    $map[40] = 'Oem1'      # ù %
    $map[43] = 'Oem7'      # * µ (scancode 2B)
    # Bottom row
    $map[42] = 'LShiftKey'
    $map[86] = 'OemBackslash' # < >
    $map[44] = 'W'
    $map[45] = 'X'
    $map[46] = 'C'
    $map[47] = 'V'
    $map[48] = 'B'
    $map[49] = 'N'
    $map[50] = 'Oemcomma'   # , ?
    $map[51] = 'OemPeriod'  # ; .
    $map[52] = 'OemQuestion'# : /
    $map[53] = 'Oem8'       # ! § (VK Oem8 on FR)
    $map[54] = 'RShiftKey'
    # Mods / space
    $map[29] = 'LControlKey'
    $map[56] = 'LMenu'
    $map[57] = 'Space'
    return $map
}

function Get-FrLayoutRows {
    return @(
        @('Escape','F1','F2','F3','F4','F5','F6','F7','F8','F9','F10','F11','F12','PrintScreen','Scroll','Pause'),
        @('Oemtilde','D1','D2','D3','D4','D5','D6','D7','D8','D9','D0','OemMinus','Oemplus','Back'),
        @('Tab','A','Z','E','R','T','Y','U','I','O','P','OemOpenBrackets','Oem6'),
        @('CapsLock','Q','S','D','F','G','H','J','K','L','M','Oem1','Oem7','Return'),
        @('LShiftKey','OemBackslash','W','X','C','V','B','N','Oemcomma','OemPeriod','OemQuestion','Oem8','RShiftKey'),
        @('LControlKey','LWin','LMenu','Space','RMenu','RWin','Apps','RControlKey')
    )
}

function Get-FrLabelOverrides {
    return @{
        'Oemtilde'        = '²'
        'D1'              = '& 1'
        'D2'              = 'é 2'
        'D3'              = '" 3'
        'D4'              = "' 4"
        'D5'              = '( 5'
        'D6'              = '- 6'
        'D7'              = 'è 7'
        'D8'              = '_ 8'
        'D9'              = 'ç 9'
        'D0'              = 'à 0'
        'OemMinus'        = ') °'
        'Oemplus'         = '= +'
        'OemOpenBrackets' = '^ ¨'
        'Oem6'            = '$ £'
        'Oem5'            = '< >'
        'Oem1'            = 'ù %'
        'Oem7'            = '* µ'
        'Oemcomma'        = ', ?'
        'OemPeriod'       = '; .'
        'OemQuestion'     = ': /'
        'Oem8'            = '! §'
        'OemBackslash'    = '< >'
        'Oem102'          = '< >'
        'RMenu'           = 'AltGr'
    }
}

function Get-UsScanKeyMap {
    $map = @{}
    $map[41] = 'Oemtilde'
    $map[2]  = 'D1'
    $map[3]  = 'D2'
    $map[4]  = 'D3'
    $map[5]  = 'D4'
    $map[6]  = 'D5'
    $map[7]  = 'D6'
    $map[8]  = 'D7'
    $map[9]  = 'D8'
    $map[10] = 'D9'
    $map[11] = 'D0'
    $map[12] = 'OemMinus'
    $map[13] = 'Oemplus'
    $map[14] = 'Back'
    $map[15] = 'Tab'
    $map[16] = 'Q'
    $map[17] = 'W'
    $map[18] = 'E'
    $map[19] = 'R'
    $map[20] = 'T'
    $map[21] = 'Y'
    $map[22] = 'U'
    $map[23] = 'I'
    $map[24] = 'O'
    $map[25] = 'P'
    $map[26] = 'OemOpenBrackets'
    $map[27] = 'Oem6'
    $map[28] = 'Return'
    $map[58] = 'CapsLock'
    $map[30] = 'A'
    $map[31] = 'S'
    $map[32] = 'D'
    $map[33] = 'F'
    $map[34] = 'G'
    $map[35] = 'H'
    $map[36] = 'J'
    $map[37] = 'K'
    $map[38] = 'L'
    $map[39] = 'Oem1'
    $map[40] = 'Oem7'
    $map[42] = 'LShiftKey'
    $map[43] = 'Oem5'        # backslash/pipe
    $map[44] = 'Z'
    $map[45] = 'X'
    $map[46] = 'C'
    $map[47] = 'V'
    $map[48] = 'B'
    $map[49] = 'N'
    $map[50] = 'M'
    $map[51] = 'Oemcomma'
    $map[52] = 'OemPeriod'
    $map[53] = 'OemQuestion'
    $map[54] = 'RShiftKey'
    $map[29] = 'LControlKey'
    $map[56] = 'LMenu'
    $map[57] = 'Space'
    return $map
}

function Get-UsLayoutRows {
    return @(
        @('Escape','F1','F2','F3','F4','F5','F6','F7','F8','F9','F10','F11','F12','PrintScreen','Scroll','Pause'),
        @('Oemtilde','D1','D2','D3','D4','D5','D6','D7','D8','D9','D0','OemMinus','Oemplus','Back'),
        @('Tab','Q','W','E','R','T','Y','U','I','O','P','OemOpenBrackets','Oem6','Oem5'),
        @('CapsLock','A','S','D','F','G','H','J','K','L','Oem1','Oem7','Return'),
        @('LShiftKey','Z','X','C','V','B','N','M','Oemcomma','OemPeriod','OemQuestion','RShiftKey'),
        @('LControlKey','LWin','LMenu','Space','RMenu','RWin','Apps','RControlKey')
    )
}

function Get-UsLabelOverrides {
    return @{
        'Oemtilde'        = '` ~'
        'OemMinus'        = '-'
        'Oemplus'         = '='
        'OemOpenBrackets' = '['
        'Oem6'            = ']'
        'Oem5'            = '\'
        'Oem1'            = ';'
        'Oem7'            = "'"
        'Oemcomma'        = ','
        'OemPeriod'       = '.'
        'OemQuestion'     = '/'
    }
}

function Get-NumpadRows {
    return @(
        @('NumLock','Divide','Multiply','Subtract'),
        @('NumPad7','NumPad8','NumPad9','Add'),
        @('NumPad4','NumPad5','NumPad6'),
        @('NumPad1','NumPad2','NumPad3','Return'),
        @('NumPad0','Decimal')
    )
}

function Get-UkScanKeyMap {
    $map = @{}
    $map[41] = 'Oemtilde'
    $map[2]  = 'D1'
    $map[3]  = 'D2'
    $map[4]  = 'D3'
    $map[5]  = 'D4'
    $map[6]  = 'D5'
    $map[7]  = 'D6'
    $map[8]  = 'D7'
    $map[9]  = 'D8'
    $map[10] = 'D9'
    $map[11] = 'D0'
    $map[12] = 'OemMinus'
    $map[13] = 'Oemplus'
    $map[14] = 'Back'
    $map[15] = 'Tab'
    $map[16] = 'Q'
    $map[17] = 'W'
    $map[18] = 'E'
    $map[19] = 'R'
    $map[20] = 'T'
    $map[21] = 'Y'
    $map[22] = 'U'
    $map[23] = 'I'
    $map[24] = 'O'
    $map[25] = 'P'
    $map[26] = 'OemOpenBrackets'
    $map[27] = 'Oem6'
    $map[28] = 'Return'
    $map[58] = 'CapsLock'
    $map[30] = 'A'
    $map[31] = 'S'
    $map[32] = 'D'
    $map[33] = 'F'
    $map[34] = 'G'
    $map[35] = 'H'
    $map[36] = 'J'
    $map[37] = 'K'
    $map[38] = 'L'
    $map[39] = 'Oem1'
    $map[40] = 'Oem7'        # '#'
    $map[42] = 'LShiftKey'
    $map[86] = 'Oem102'      # \ | (ISO key)
    $map[43] = 'Oem5'        # backslash
    $map[44] = 'Z'
    $map[45] = 'X'
    $map[46] = 'C'
    $map[47] = 'V'
    $map[48] = 'B'
    $map[49] = 'N'
    $map[50] = 'M'
    $map[51] = 'Oemcomma'
    $map[52] = 'OemPeriod'
    $map[53] = 'OemQuestion'
    $map[54] = 'RShiftKey'
    $map[29] = 'LControlKey'
    $map[56] = 'LMenu'
    $map[57] = 'Space'
    return $map
}

function Get-UkLayoutRows {
    return @(
        @('Escape','F1','F2','F3','F4','F5','F6','F7','F8','F9','F10','F11','F12','PrintScreen','Scroll','Pause'),
        @('Oemtilde','D1','D2','D3','D4','D5','D6','D7','D8','D9','D0','OemMinus','Oemplus','Back'),
        @('Tab','Q','W','E','R','T','Y','U','I','O','P','OemOpenBrackets','Oem6','Oem5'),
        @('CapsLock','A','S','D','F','G','H','J','K','L','Oem1','Oem7','Return'),
        @('LShiftKey','Oem102','Z','X','C','V','B','N','M','Oemcomma','OemPeriod','OemQuestion','RShiftKey'),
        @('LControlKey','LWin','LMenu','Space','RMenu','RWin','Apps','RControlKey')
    )
}

function Get-UkLabelOverrides {
    return @{
        'Oemtilde'        = '` ¬'
        'OemMinus'        = '- _'
        'Oemplus'         = '= +'
        'OemOpenBrackets' = '[ {'
        'Oem6'            = '] }'
        'Oem5'            = '\ |'
        'Oem102'          = '\ |'
        'Oem1'            = '; :'
        'Oem7'            = '# ~'
        'Oemcomma'        = ', <'
        'OemPeriod'       = '. >'
        'OemQuestion'     = '/ ?'
    }
}

function Get-LayoutDefinition {
    param(
        [string]$Name,
        [string]$CustomConfigPath
    )
    $lower = $Name.ToLowerInvariant()
    switch ($lower) {
        {$_ -in @('fr','fr-azerty','azerty')} {
            return [pscustomobject]@{
                LayoutRows     = Get-FrLayoutRows
                ScanMap        = Get-FrScanKeyMap
                WidthOverrides = Get-CommonWidthOverrides
                Labels         = Get-FrLabelOverrides
                Name           = 'fr-azerty'
            }
        }
        {$_ -in @('us','us-qwerty','qwerty','en-us')} {
            return [pscustomobject]@{
                LayoutRows     = Get-UsLayoutRows
                ScanMap        = Get-UsScanKeyMap
                WidthOverrides = Get-CommonWidthOverrides
                Labels         = Get-UsLabelOverrides
                Name           = 'us-qwerty'
            }
        }
        {$_ -in @('uk','uk-qwerty','en-gb','gb')} {
            return [pscustomobject]@{
                LayoutRows     = Get-UkLayoutRows
                ScanMap        = Get-UkScanKeyMap
                WidthOverrides = Get-CommonWidthOverrides
                Labels         = Get-UkLabelOverrides
                Name           = 'uk-qwerty'
            }
        }
        default {
            if ([string]::IsNullOrWhiteSpace($CustomConfigPath)) {
                Write-Warning "Layout '$Name' inconnu. Utilisation du layout fr-azerty par défaut."
                return [pscustomobject]@{
                    LayoutRows     = Get-FrLayoutRows
                    ScanMap        = Get-FrScanKeyMap
                    WidthOverrides = Get-CommonWidthOverrides
                    Labels         = Get-FrLabelOverrides
                    Name           = 'fr-azerty'
                }
            }
            if (-not (Test-Path $CustomConfigPath)) {
                Write-Warning "Layout '$Name' inconnu et fichier $CustomConfigPath introuvable. Utilisation du layout fr-azerty."
                return [pscustomobject]@{
                    LayoutRows     = Get-FrLayoutRows
                    ScanMap        = Get-FrScanKeyMap
                    WidthOverrides = Get-CommonWidthOverrides
                    Labels         = Get-FrLabelOverrides
                    Name           = 'fr-azerty'
                }
            }
            try {
                $raw = Get-Content -Raw -Path $CustomConfigPath | ConvertFrom-Json
                if (-not $raw.layoutRows -or -not $raw.scanMap) {
                    throw "layoutRows or scanMap missing in $CustomConfigPath"
                }
                $scan = @{}
                foreach ($k in $raw.scanMap.PSObject.Properties) {
                    $intKey = [int]$k.Name
                    $scan[$intKey] = [string]$k.Value
                }
                $widths = if ($raw.widthOverrides) { @{} + $raw.widthOverrides } else { Get-CommonWidthOverrides }
                $labels = if ($raw.labels) { @{} + $raw.labels } else { @{} }
                return [pscustomobject]@{
                    LayoutRows     = $raw.layoutRows
                    ScanMap        = $scan
                    WidthOverrides = $widths
                    Labels         = $labels
                    Name           = $Name
                }
            } catch {
                Write-Warning "Impossible de charger le layout custom $CustomConfigPath : $_. Utilisation du layout fr-azerty."
                return [pscustomobject]@{
                    LayoutRows     = Get-FrLayoutRows
                    ScanMap        = Get-FrScanKeyMap
                    WidthOverrides = Get-CommonWidthOverrides
                    Labels         = Get-FrLabelOverrides
                    Name           = 'fr-azerty'
                }
            }
        }
    }
}

function Get-KeyLabelText {
    param([string]$Name)
    if ($script:labelOverrides -and $script:labelOverrides.ContainsKey($Name)) {
        return $script:labelOverrides[$Name]
    }
    if ($Name -match '^D(\d)$') { return $Matches[1] }
    switch ($Name) {
        'NumLock'     { return 'Num' }
        'Divide'      { return '/' }
        'Multiply'    { return '*' }
        'Subtract'    { return '-' }
        'Add'         { return '+' }
        'Decimal'     { return '.' }
        'NumPad0'     { return '0' }
        'NumPad1'     { return '1' }
        'NumPad2'     { return '2' }
        'NumPad3'     { return '3' }
        'NumPad4'     { return '4' }
        'NumPad5'     { return '5' }
        'NumPad6'     { return '6' }
        'NumPad7'     { return '7' }
        'NumPad8'     { return '8' }
        'NumPad9'     { return '9' }
        'Escape'      { return 'Esc' }
        'Back'        { return 'Backspace' }
        'Return'      { return 'Enter' }
        'PrintScreen' { return 'PrtSc' }
        'Scroll'      { return 'ScrLk' }
        'LMenu'       { return 'Alt' }
        'RMenu'       { return 'AltGr' }
        'LWin'        { return 'Win' }
        'RWin'        { return 'Win' }
        'Apps'        { return 'Menu' }
        default       { return $Name }
    }
}

function Normalize-KeyName {
    param([string]$Name)
    switch ($Name) {
        'ControlKey' { return 'LControlKey' }
        'ShiftKey'   { return 'LShiftKey' }
        'Menu'       { return 'LMenu' }
        'Esc'        { return 'Escape' }
        'Snapshot'   { return 'PrintScreen' }
        'Capital'    { return 'CapsLock' }
        'Oem3'       { return 'Oemtilde' }
        'Oem102'     { return 'OemBackslash' }
        default      { return $Name }
    }
}

function Get-VerificationRatio {
    param([pscustomobject]$Visualizer)

    if (-not $Visualizer -or -not $Visualizer.Required) { return 1 }
    $requiredCount = $Visualizer.Required.Count
    if ($requiredCount -le 0) { return 1 }
    $verifiedCount = 0
    foreach ($key in $Visualizer.Required) {
        if ($Visualizer.Verified.Contains($key)) { $verifiedCount++ }
    }
    return $verifiedCount / $requiredCount
}

function Update-OkButtonState {
    param([pscustomobject]$Visualizer)

    if (-not $Visualizer -or -not $Visualizer.OkButton) { return }
    $ratio = Get-VerificationRatio -Visualizer $Visualizer
    $percent = [math]::Round($ratio * 100)
    $Visualizer.OkButton.Enabled = ($ratio -ge $script:minOkRatio)
    if ($Visualizer.OkButton.Enabled) {
        $Visualizer.OkButton.Text = 'OK'
    } else {
        $Visualizer.OkButton.Text = "OK ($percent%)"
    }
}

function New-KeyboardVisualizer {
    param(
        [System.Collections.IEnumerable]$LayoutRows,
        [hashtable]$ScanMap,
        [hashtable]$WidthOverrides,
        [hashtable]$LabelOverrides,
        [string[]]$LayoutOptions,
        [string]$SelectedLayout,
        [string[]]$NumpadOptions,
        [string]$SelectedNumpad
    )
    [System.Windows.Forms.Application]::EnableVisualStyles() | Out-Null

    $form = New-Object System.Windows.Forms.Form
    $form.Text = "Keyboard visualizer"
    $form.StartPosition = 'CenterScreen'
    $form.Size = New-Object System.Drawing.Size(1150, 420)
    $form.WindowState = 'Maximized'
    $form.FormBorderStyle = 'None'
    $form.BackColor = [System.Drawing.Color]::FromArgb(18,18,18)
    $form.ForeColor = [System.Drawing.Color]::White
    $form.AutoScroll = $true

    $defaultBack = [System.Drawing.Color]::FromArgb(45,45,45)
    $defaultFore = [System.Drawing.Color]::White
    $activeBack = [System.Drawing.Color]::FromArgb(46,204,113)
    $activeFore = [System.Drawing.Color]::Black

    $root = New-Object System.Windows.Forms.FlowLayoutPanel
    $root.Dock = 'Fill'
    $root.FlowDirection = 'TopDown'
    $root.WrapContents = $false
    $root.AutoSize = $true
    $root.AutoSizeMode = 'GrowAndShrink'
    $root.Padding = '10,10,10,10'

    $header = New-Object System.Windows.Forms.FlowLayoutPanel
    $header.FlowDirection = 'LeftToRight'
    $header.WrapContents = $false
    $header.AutoSize = $true
    $header.AutoSizeMode = 'GrowAndShrink'
    $header.Margin = '0,0,0,10'

    $lblLayout = New-Object System.Windows.Forms.Label
    $lblLayout.Text = "Layout"
    $lblLayout.ForeColor = [System.Drawing.Color]::White
    $lblLayout.Font = New-Object System.Drawing.Font('Segoe UI',10,[System.Drawing.FontStyle]::Regular)
    $lblLayout.AutoSize = $true
    $lblLayout.Margin = '0,6,6,0'
    $header.Controls.Add($lblLayout)

    $combo = New-Object System.Windows.Forms.ComboBox
    $combo.DropDownStyle = 'DropDownList'
    $combo.Width = 200
    $combo.TabStop = $false
    $combo.add_KeyDown({ param($s,$e) $e.Handled = $true; $e.SuppressKeyPress = $true })
    $combo.add_KeyPress({ param($s,$e) $e.Handled = $true })
    foreach ($opt in $LayoutOptions) { [void]$combo.Items.Add($opt) }
    if ($combo.Items.Contains($SelectedLayout)) { $combo.SelectedItem = $SelectedLayout } else { if ($combo.Items.Count -gt 0) { $combo.SelectedIndex = 0 } }
    $header.Controls.Add($combo)

    $lblNum = New-Object System.Windows.Forms.Label
    $lblNum.Text = "Pavé numérique"
    $lblNum.ForeColor = [System.Drawing.Color]::White
    $lblNum.Font = New-Object System.Drawing.Font('Segoe UI',10,[System.Drawing.FontStyle]::Regular)
    $lblNum.AutoSize = $true
    $lblNum.Margin = '12,6,6,0'
    $header.Controls.Add($lblNum)

    $comboNum = New-Object System.Windows.Forms.ComboBox
    $comboNum.DropDownStyle = 'DropDownList'
    $comboNum.Width = 130
    $comboNum.TabStop = $false
    $comboNum.add_KeyDown({ param($s,$e) $e.Handled = $true; $e.SuppressKeyPress = $true })
    $comboNum.add_KeyPress({ param($s,$e) $e.Handled = $true })
    foreach ($opt in $NumpadOptions) { [void]$comboNum.Items.Add($opt) }
    if ($comboNum.Items.Contains($SelectedNumpad)) { $comboNum.SelectedItem = $SelectedNumpad } else { if ($comboNum.Items.Count -gt 0) { $comboNum.SelectedIndex = 0 } }
    $header.Controls.Add($comboNum)

    $kbPanel = New-Object System.Windows.Forms.FlowLayoutPanel
    $kbPanel.Dock = 'Top'
    $kbPanel.FlowDirection = 'TopDown'
    $kbPanel.WrapContents = $false
    $kbPanel.AutoSize = $true
    $kbPanel.AutoSizeMode = 'GrowAndShrink'
    $kbPanel.Padding = '0,0,0,0'

    $footer = New-Object System.Windows.Forms.FlowLayoutPanel
    $footer.FlowDirection = 'LeftToRight'
    $footer.WrapContents = $false
    $footer.AutoSize = $true
    $footer.AutoSizeMode = 'GrowAndShrink'
    $footer.Margin = '0,12,0,0'
    $footer.Padding = '0,0,0,0'

    $okButton = New-Object System.Windows.Forms.Button
    $okButton.Text = "OK"
    $okButton.AutoSize = $true
    $okButton.Padding = '8,6,8,6'
    $okButton.BackColor = [System.Drawing.Color]::FromArgb(46,204,113)
    $okButton.ForeColor = [System.Drawing.Color]::Black
    $okButton.FlatStyle = 'Flat'
    $okButton.Font = New-Object System.Drawing.Font('Segoe UI',10,[System.Drawing.FontStyle]::Bold)
    $okButton.TabStop = $false
    $okButton.UseMnemonic = $false
    $okButton.UseVisualStyleBackColor = $false

    $nokButton = New-Object System.Windows.Forms.Button
    $nokButton.Text = "NOK"
    $nokButton.AutoSize = $true
    $nokButton.Padding = '8,6,8,6'
    $nokButton.BackColor = [System.Drawing.Color]::FromArgb(231,76,60)
    $nokButton.ForeColor = [System.Drawing.Color]::White
    $nokButton.FlatStyle = 'Flat'
    $nokButton.Font = New-Object System.Drawing.Font('Segoe UI',10,[System.Drawing.FontStyle]::Bold)
    $nokButton.TabStop = $false
    $nokButton.UseMnemonic = $false
    $nokButton.UseVisualStyleBackColor = $false

    $padLabel = New-Object System.Windows.Forms.Label
    $padLabel.Text = "Pave tactile"
    $padLabel.ForeColor = [System.Drawing.Color]::White
    $padLabel.Font = New-Object System.Drawing.Font('Segoe UI',10,[System.Drawing.FontStyle]::Regular)
    $padLabel.AutoSize = $true
    $padLabel.Margin = '18,8,6,0'

    $padCombo = New-Object System.Windows.Forms.ComboBox
    $padCombo.DropDownStyle = 'DropDownList'
    $padCombo.Width = 120
    $padCombo.TabStop = $false
    $padCombo.Margin = '0,4,0,0'
    $padCombo.add_KeyDown({ param($s,$e) $e.Handled = $true; $e.SuppressKeyPress = $true })
    $padCombo.add_KeyPress({ param($s,$e) $e.Handled = $true })
    [void]$padCombo.Items.Add('Non teste')
    [void]$padCombo.Items.Add('OK')
    [void]$padCombo.Items.Add('NOK')
    $padCombo.SelectedIndex = 0

    $footer.Controls.Add($okButton)
    $footer.Controls.Add($nokButton)
    $footer.Controls.Add($padLabel)
    $footer.Controls.Add($padCombo)

    $root.Controls.Add($header)
    $root.Controls.Add($kbPanel)
    $root.Controls.Add($footer)

    $form.Controls.Add($root)

    $visualizer = [pscustomobject]@{
        Form            = $form
        RootPanel       = $root
        HeaderPanel     = $header
        LayoutSelector  = $combo
        NumpadSelector  = $comboNum
        KbPanel         = $kbPanel
        Controls        = @{}
        OtherPanel      = $null
        DefaultBack     = $defaultBack
        DefaultFore     = $defaultFore
        ActiveBack      = $activeBack
        ActiveFore      = $activeFore
        ScanToKey       = $null
        Verified        = [System.Collections.Generic.HashSet[string]]::new()
        Required        = [System.Collections.Generic.HashSet[string]]::new()
        OkButton        = $okButton
        NokButton       = $nokButton
        PadStatusSelector = $padCombo
        WidthOverrides  = $WidthOverrides
        LabelOverrides  = $LabelOverrides
    }
    Update-OkButtonState -Visualizer $visualizer
    return $visualizer
}

function Rebuild-KeyboardLayout {
    param(
        [pscustomobject]$Visualizer,
        [pscustomobject]$LayoutDef,
        [System.Collections.IEnumerable]$NumpadRows
    )
    $kbPanel = $Visualizer.KbPanel
    $kbPanel.Controls.Clear()

    $Visualizer.WidthOverrides = if ($LayoutDef.WidthOverrides) { $LayoutDef.WidthOverrides } else { Get-CommonWidthOverrides }
    $Visualizer.LabelOverrides = if ($LayoutDef.Labels) { $LayoutDef.Labels } else { @{} }
    $script:labelOverrides = $Visualizer.LabelOverrides

    $layout = @()
    $layout += $LayoutDef.LayoutRows
    if ($NumpadRows) { $layout += $NumpadRows }
    $allKeysSet = [System.Collections.Generic.HashSet[string]]::new()
    foreach ($row in $layout) {
        foreach ($k in $row) { $null = $allKeysSet.Add($k) }
    }

    $controls = @{}
    $Visualizer.ScanToKey = $LayoutDef.ScanMap
    $Visualizer.Verified.Clear()
    $Visualizer.Required = $allKeysSet
    Update-OkButtonState -Visualizer $Visualizer

    function New-KeyLabelInner {
        param([string]$KeyName, $VisualizerRef)
        $label = New-Object System.Windows.Forms.Label
        $label.Text = Get-KeyLabelText $KeyName
        $label.TextAlign = 'MiddleCenter'
        $label.AutoSize = $false
        $label.BorderStyle = 'FixedSingle'
        $label.Margin = '3,3,3,3'
        $label.Padding = '4,4,4,4'
        $label.Height = 42
        $label.Width = if ($VisualizerRef.WidthOverrides.ContainsKey($KeyName)) { $VisualizerRef.WidthOverrides[$KeyName] } else { 60 }
        $label.Font = New-Object System.Drawing.Font('Segoe UI',10,[System.Drawing.FontStyle]::Regular)
        $label.BackColor = $VisualizerRef.DefaultBack
        $label.ForeColor = $VisualizerRef.DefaultFore
        return $label
    }

    foreach ($row in $layout) {
        $rowPanel = New-Object System.Windows.Forms.FlowLayoutPanel
        $rowPanel.FlowDirection = 'LeftToRight'
        $rowPanel.WrapContents = $false
        $rowPanel.AutoSize = $true
        $rowPanel.AutoSizeMode = 'GrowAndShrink'
        $rowPanel.Margin = '0,4,0,0'

        foreach ($keyName in $row) {
            $label = New-KeyLabelInner -KeyName $keyName -VisualizerRef $Visualizer
            $rowPanel.Controls.Add($label)
            $controls[$keyName] = $label
        }
        $kbPanel.Controls.Add($rowPanel)
    }

    $otherLabel = New-Object System.Windows.Forms.Label
    $otherLabel.Text = "Autres touches"
    $otherLabel.ForeColor = [System.Drawing.Color]::Gainsboro
    $otherLabel.Font = New-Object System.Drawing.Font('Segoe UI',9,[System.Drawing.FontStyle]::Bold)
    $otherLabel.AutoSize = $true
    $otherLabel.Margin = '0,10,0,0'
    $kbPanel.Controls.Add($otherLabel)

    $otherPanel = New-Object System.Windows.Forms.FlowLayoutPanel
    $otherPanel.FlowDirection = 'LeftToRight'
    $otherPanel.WrapContents = $true
    $otherPanel.AutoSize = $true
    $otherPanel.AutoSizeMode = 'GrowAndShrink'
    $otherPanel.Margin = '0,4,0,0'
    $kbPanel.Controls.Add($otherPanel)

    $Visualizer.Controls = $controls
    $Visualizer.OtherPanel = $otherPanel
}

function Update-KeyboardVisualizer {
    param(
        [pscustomobject]$Visualizer,
        [string]$KeyName,
        [int]$Msg,
        [int]$Scan
    )

    if (-not $Visualizer -or -not $Visualizer.Form -or $Visualizer.Form.IsDisposed) { return }

    $isDown = switch ($Msg) {
        256 { $true }   # WM_KEYDOWN
        260 { $true }   # WM_SYSKEYDOWN
        257 { $false }  # WM_KEYUP
        261 { $false }  # WM_SYSKEYUP
        default { $null }
    }
    if ($isDown -eq $null) { return }

    $normalized = Normalize-KeyName $KeyName

    $preferScanMap = $true
    if ($normalized -in @('RControlKey','RMenu','RShiftKey','RWin')) {
        $preferScanMap = $false
    }

    if ($preferScanMap -and $Visualizer.ScanToKey.ContainsKey($Scan)) {
        $normalized = $Visualizer.ScanToKey[$Scan]
    }
    $control = $Visualizer.Controls[$normalized]

    if (-not $control) {
        # Try right-side variants when we only got generic names.
        if ($normalized -eq 'LControlKey') { $control = $Visualizer.Controls['RControlKey'] }
        elseif ($normalized -eq 'LShiftKey') { $control = $Visualizer.Controls['RShiftKey'] }
        elseif ($normalized -eq 'LMenu') { $control = $Visualizer.Controls['RMenu'] }
    }

    if (-not $control) {
        $control = New-Object System.Windows.Forms.Label
        $control.Text = Get-KeyLabelText $normalized
        $control.TextAlign = 'MiddleCenter'
        $control.AutoSize = $false
        $control.Width = 70
        $control.Height = 40
        $control.Margin = '3,3,3,3'
        $control.BorderStyle = 'FixedSingle'
        $control.Font = New-Object System.Drawing.Font('Segoe UI',10,[System.Drawing.FontStyle]::Regular)
        $control.BackColor = $Visualizer.DefaultBack
        $control.ForeColor = $Visualizer.DefaultFore
        $Visualizer.OtherPanel.Controls.Add($control)
        $Visualizer.Controls[$normalized] = $control
    }

    if ($isDown) {
        $null = $Visualizer.Verified.Add($normalized)
        Update-OkButtonState -Visualizer $Visualizer
    }

    $isVerified = $Visualizer.Verified.Contains($normalized)
    if ($isDown -or $isVerified) {
        $control.BackColor = $Visualizer.ActiveBack
        $control.ForeColor = $Visualizer.ActiveFore
    } else {
        $control.BackColor = $Visualizer.DefaultBack
        $control.ForeColor = $Visualizer.DefaultFore
    }
}

$handler = [WinAPI.GlobalKeyboardHook+KeyboardEvent]{
    param($vk, $scan, $flags, $msg)
    $now = (Get-Date).ToUniversalTime().ToString("o")
    $name = [System.Enum]::GetName([System.Windows.Forms.Keys], [int]$vk)
    if (-not $name) { $name = "VK_$vk" }
    if ($msg -eq 257 -and $script:stopRequested) { return }
    $queue.Add([pscustomobject]@{
        ts    = $now
        vk    = [int]$vk
        scan  = [int]$scan
        name  = $name
        flags = [int]$flags
        msg   = [int]$msg
    })
    return $script:blockInputs
}

function Resolve-LogPath {
    param([string]$Path)
    if (-not $Path) {
        return Join-Path $env:TEMP "workflow\\keyboard\\keyboard_log.jsonl"
    }
    $base = if ($PSScriptRoot) { $PSScriptRoot } elseif ($PSCommandPath) { Split-Path $PSCommandPath -Parent } else { (Get-Location).Path }
    if (-not $base) { $base = $env:TEMP }
    # Keep relative paths relative to the script location; create directory if missing.
    $full = if ([System.IO.Path]::IsPathRooted($Path)) { $Path } else { Join-Path $base $Path }
    $dir = [System.IO.Path]::GetDirectoryName($full)
    if ($dir -and -not (Test-Path $dir)) {
        New-Item -ItemType Directory -Force -Path $dir | Out-Null
    }
    return $full
}

function Resolve-ConfigDir {
    param([string]$Path)

    $base = if ($PSScriptRoot) { $PSScriptRoot } elseif ($PSCommandPath) { Split-Path $PSCommandPath -Parent } else { (Get-Location).Path }
    if (-not $base) { $base = $env:TEMP }
    if ([string]::IsNullOrWhiteSpace($Path)) {
        $Path = Join-Path $base 'keyboard conf'
    }
    $hasDrive = $Path -match '^[A-Za-z]:'
    $hasUnc = $Path -match '^\\\\[^\\]+\\'
    if (-not $hasDrive -and -not $hasUnc) {
        $Path = Join-Path $base ($Path.TrimStart('\'))
    }
    if (-not $Path -or $Path -eq '\' -or $Path -eq '\\') {
        $Path = Join-Path $env:TEMP 'workflow\\keyboard\\conf'
    }
    if (-not (Test-Path $Path)) { New-Item -ItemType Directory -Force -Path $Path | Out-Null }
    return $Path
}

function Open-LogStream {
    param([string]$Path)

    $dir = [System.IO.Path]::GetDirectoryName($Path)
    if ($dir -and -not (Test-Path $dir)) {
        New-Item -ItemType Directory -Force -Path $dir | Out-Null
    }
    try {
        $fs = New-Object System.IO.FileStream($Path, [System.IO.FileMode]::Append, [System.IO.FileAccess]::Write, [System.IO.FileShare]::ReadWrite)
        $writer = New-Object System.IO.StreamWriter($fs, [System.Text.Encoding]::UTF8)
        $writer.AutoFlush = $true
        return @{ Path = $Path; Writer = $writer }
    } catch {
        $fallback = Join-Path $dir ("keyboard_log_{0}_{1}.jsonl" -f (Get-Date -Format "yyyyMMdd_HHmmss"), $PID)
        try {
            $fs = New-Object System.IO.FileStream($fallback, [System.IO.FileMode]::Append, [System.IO.FileAccess]::Write, [System.IO.FileShare]::ReadWrite)
            $writer = New-Object System.IO.StreamWriter($fs, [System.Text.Encoding]::UTF8)
            $writer.AutoFlush = $true
            Write-Warning "Log file locked, using fallback: $fallback"
            return @{ Path = $fallback; Writer = $writer }
        } catch {
            Write-Warning "Cannot open log file: $($_.Exception.Message)"
            return $null
        }
    }
}

$logPathResolved = Resolve-LogPath -Path $LogPath
$configDirResolved = Resolve-ConfigDir -Path $ConfigDir
$configFileResolved = if ([string]::IsNullOrWhiteSpace($ConfigPath)) {
    $stamp = Get-Date -Format "yyyyMMdd_HHmmss"
    Join-Path $configDirResolved ("keyboard_{0}.json" -f $stamp)
} else {
    if ([System.IO.Path]::IsPathRooted($ConfigPath)) { $ConfigPath } else { Join-Path $configDirResolved $ConfigPath }
}
$layoutDef = Get-LayoutDefinition -Name $Layout -CustomConfigPath $LayoutConfig
$layoutOptions = @('fr-azerty','us-qwerty','uk-qwerty')
if (-not [string]::IsNullOrWhiteSpace($LayoutConfig)) {
    if (-not ($layoutOptions -contains $Layout)) { $layoutOptions += $Layout }
}
if (-not ($layoutOptions -contains $layoutDef.Name)) { $layoutOptions += $layoutDef.Name }
$numpadOptions = @('Sans','Avec pavé num')
function Get-SelectedNumpadRows($sel) {
    if ($sel -eq 'Avec pavé num') { return Get-NumpadRows }
    return @()
}
$selectedNumpad = $numpadOptions[0]

[System.Windows.Forms.Application]::EnableVisualStyles() | Out-Null
$visualizer = New-KeyboardVisualizer -LayoutRows $layoutDef.LayoutRows -ScanMap $layoutDef.ScanMap -WidthOverrides $layoutDef.WidthOverrides -LabelOverrides $layoutDef.Labels -LayoutOptions $layoutOptions -SelectedLayout $layoutDef.Name -NumpadOptions $numpadOptions -SelectedNumpad $selectedNumpad
$visualizer.LayoutSelector.add_SelectedIndexChanged({
    $selected = $visualizer.LayoutSelector.SelectedItem
    if (-not $selected) { return }
    $newLayout = Get-LayoutDefinition -Name $selected -CustomConfigPath $LayoutConfig
    $npSel = $visualizer.NumpadSelector.SelectedItem
    $npRows = Get-SelectedNumpadRows -sel $npSel
    Rebuild-KeyboardLayout -Visualizer $visualizer -LayoutDef $newLayout -NumpadRows $npRows
    Write-Host "[info] Layout sélectionné: $($newLayout.Name) / Numpad: $npSel"
})
$visualizer.NumpadSelector.add_SelectedIndexChanged({
    $npSel = $visualizer.NumpadSelector.SelectedItem
    $selected = $visualizer.LayoutSelector.SelectedItem
    $layoutSel = Get-LayoutDefinition -Name $selected -CustomConfigPath $LayoutConfig
    $npRows = Get-SelectedNumpadRows -sel $npSel
    Rebuild-KeyboardLayout -Visualizer $visualizer -LayoutDef $layoutSel -NumpadRows $npRows
    Write-Host "[info] Numpad sélectionné: $npSel"
})
$visualizer.PadStatusSelector.add_SelectedIndexChanged({
    $selected = $visualizer.PadStatusSelector.SelectedItem
    switch ($selected) {
        'OK' { $script:padStatus = 'ok' }
        'NOK' { $script:padStatus = 'nok' }
        default { $script:padStatus = 'not_tested' }
    }
    Write-Host "[info] Pavé tactile: $script:padStatus"
})
$initialNpRows = Get-SelectedNumpadRows -sel $selectedNumpad
Rebuild-KeyboardLayout -Visualizer $visualizer -LayoutDef $layoutDef -NumpadRows $initialNpRows
$visualizer.Form.TopMost = $true
$visualizer.Form.add_Shown({
    param($sender, $eventArgs)
    if ($sender) {
        $sender.Activate()
        $sender.TopMost = $false
    }
})
$visualizer.Form.add_FormClosed({ $script:stopRequested = $true })
$visualizer.Form.Show()
$visualizer.OkButton.add_Click({
    if ($script:stopRequested) { return }
    $ratio = Get-VerificationRatio -Visualizer $visualizer
    if ($ratio -lt $script:minOkRatio) {
        $minPercent = [math]::Round($script:minOkRatio * 100)
        $percent = [math]::Round($ratio * 100)
        [System.Windows.Forms.MessageBox]::Show(
            "Testez au moins ${minPercent}% des touches avant de valider (actuel: ${percent}%).",
            "MDT Live Ops",
            [System.Windows.Forms.MessageBoxButtons]::OK,
            [System.Windows.Forms.MessageBoxIcon]::Warning
        ) | Out-Null
        return
    }
    $missing = $visualizer.Required | Where-Object { -not $visualizer.Verified.Contains($_) }
    if ($missing.Count -gt 0) {
        Write-Warning ("[warn] Touches non vues: {0}" -f ($missing -join ', '))
    }
    Write-Host "[info] Validation OK forcee."
    Write-CompletionStatus -Status "OK" -Missing $missing
    $script:stopRequested = $true
})
$visualizer.NokButton.add_Click({
    if ($script:stopRequested) { return }
    $missing = $visualizer.Required | Where-Object { -not $visualizer.Verified.Contains($_) }
    Write-Warning "[warn] Validation NOK forcee."
    Write-CompletionStatus -Status "NOK" -Missing $missing
    $script:stopRequested = $true
})

[WinAPI.GlobalKeyboardHook]::add_KeyEvent($handler)
[WinAPI.GlobalKeyboardHook]::Start()

Write-Host "[info] Capturing keyboard events. Press Ctrl+C to stop."
Write-Host "[info] Config will be written to $configFileResolved"
Write-Host "[info] Layout: $($layoutDef.Name)"
$script:stopRequested = $false
$logStreamInfo = Open-LogStream -Path $logPathResolved
if ($logStreamInfo) {
    $script:logStream = $logStreamInfo.Writer
    $logPathResolved = $logStreamInfo.Path
    Write-Host "[info] Logging to $logPathResolved"
} else {
    Write-Warning "[warn] Log stream not available; keyboard events won't be persisted."
}

try {
    while (-not $script:stopRequested) {
        $item = $null
        $hasItem = $queue.TryTake([ref]$item, 100)
        if ($hasItem -and $null -ne $item) {
            $keyId = "$($item.vk)-$($item.scan)"
            if (-not $seenKeys.Contains($keyId)) {
                $null = $seenKeys.Add($keyId)
                $keyInfo[$keyId] = [pscustomobject]@{
                    vk   = [int]$item.vk
                    scan = [int]$item.scan
                    name = "$($item.name)"
                }
            }
            $json = $item | ConvertTo-Json -Compress
            $script:logStream.WriteLine($json)
            $script:logStream.Flush()
            Update-KeyboardVisualizer -Visualizer $visualizer -KeyName $item.name -Msg $item.msg -Scan $item.scan
            Write-Host "$($item.ts) | $($item.name) (VK=$($item.vk), Scan=$($item.scan)) msg=$($item.msg) flags=$($item.flags)"
        }
        [System.Windows.Forms.Application]::DoEvents()
    }
}
catch [System.Management.Automation.PipelineStoppedException] {
    # Ignore pipeline stop (Ctrl+C) to allow cleanup
}
finally {
    $script:stopRequested = $true
    [WinAPI.GlobalKeyboardHook]::Stop()
    [WinAPI.GlobalKeyboardHook]::remove_KeyEvent($handler)
    if ($script:logStream) { $script:logStream.Dispose() }
    $queue.Dispose()
    try {
        if ($visualizer -and $visualizer.Form -and -not $visualizer.Form.IsDisposed) {
            $visualizer.Form.Close()
        }
    } catch {
        # ignore UI close issues during shutdown
    }
    try {
        $config = $keyInfo.Values | Sort-Object vk, scan
        if ($config.Count -gt 0) {
            $config | ConvertTo-Json -Depth 3 | Set-Content -Path $configFileResolved -Encoding UTF8
            Write-Host "[info] Wrote config to $configFileResolved with $($config.Count) keys."
        } else {
            Write-Warning "[warn] No key events captured; config not written."
        }
    } catch {
        Write-Warning "Could not write config to ${configFileResolved}: $_"
    }
    Write-Host "[info] Stopped."
}

$exitCode = switch ($script:finalStatus) {
    'OK' { 0 }
    'NOK' { 1 }
    default { 2 }
}
exit $exitCode
