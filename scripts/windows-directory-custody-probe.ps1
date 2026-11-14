# Copyright 2026 Nowledge
# SPDX-License-Identifier: Apache-2.0
<#
Experimental native primitive falsification only. No SDK or cold-RO qualification.
Publication/native execution requires exact-source review and Root GO.
#>
[CmdletBinding()]
param(
    [ValidateSet('Main','Contender')][string] $Mode='Main',
    [string] $OutputDirectory,
    [string] $ExpectedSourceRevision,
    [string] $FixturePath,
    [string] $FixtureTag,
    [string] $AttemptNonce,
    [string] $AttemptCase,
    [string] $ExpectedScriptSha256
)
Set-StrictMode -Version Latest
$ErrorActionPreference='Stop'
$nativeSource=@'
// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0
using System;
using System.ComponentModel;
using System.Runtime.InteropServices;
using System.Text;
using Microsoft.Win32.SafeHandles;

namespace HawdbDirectoryProbeV1
{
    public sealed class OpenResult
    {
        public SafeFileHandle Handle;
        public string Api, NtStatus, IoStatus, IoInformation;
        public uint Access, Share, Options, Disposition;
        public bool Opened, UnexpectedHandle;
        public int Error;
    }
    public sealed class Identity
    {
        public bool Ok, Directory, Reparse;
        public int Error;
        public string FailedQuery, Volume, Low, High;
        public uint Attributes, ReparseTag;
    }
    public sealed class BoolResult
    {
        public bool Ok;
        public int Error;
        public uint Flags;
    }
    [StructLayout(LayoutKind.Sequential)]
    internal struct UnicodeString { public ushort Length, MaximumLength; public IntPtr Buffer; }
    [StructLayout(LayoutKind.Sequential)]
    internal struct ObjectAttributes
    {
        public uint Length;
        public IntPtr RootDirectory, ObjectName;
        public uint Attributes;
        public IntPtr SecurityDescriptor, SecurityQualityOfService;
    }
    [StructLayout(LayoutKind.Sequential)]
    internal struct IoStatusBlock { public IntPtr StatusOrPointer; public UIntPtr Information; }
    [StructLayout(LayoutKind.Sequential)]
    internal struct FileId { public ulong Volume, Low, High; }
    [StructLayout(LayoutKind.Sequential)]
    internal struct AttributeTag { public uint Attributes, ReparseTag; }

    public static class Native
    {
        public const uint Read = 0x80000000, Write = 0x40000000;
        public const uint PrimaryShare = 6, AllShare = 7, Backup = 0x02000000;
        public const uint NtWrite = 0x40100080, NtReadWriteDiagnostic = 0x40100081;
        public const uint NtOptions = 0x00200021; // DIRECTORY|SYNCHRONOUS_NONALERT|OPEN_REPARSE_POINT
        [DllImport("kernel32.dll", CharSet=CharSet.Unicode, SetLastError=true, ExactSpelling=true)]
        private static extern SafeFileHandle CreateFileW(string name, uint access, uint share,
            IntPtr security, uint disposition, uint flags, IntPtr template);
        [DllImport("kernel32.dll", SetLastError=true, ExactSpelling=true)]
        private static extern SafeFileHandle ReOpenFile(SafeFileHandle original, uint access, uint share, uint flags);
        [DllImport("ntdll.dll", ExactSpelling=true)]
        private static extern int NtCreateFile(ref IntPtr handle, uint access,
            ref ObjectAttributes attributes, ref IoStatusBlock status, IntPtr allocation,
            uint fileAttributes, uint share, uint disposition, uint options, IntPtr ea, uint eaLength);
        [DllImport("ntdll.dll", ExactSpelling=true)]
        private static extern uint RtlNtStatusToDosError(int status);
        [DllImport("kernel32.dll", SetLastError=true, ExactSpelling=true)]
        [return: MarshalAs(UnmanagedType.Bool)]
        private static extern bool GetFileInformationByHandleEx(SafeFileHandle handle, int kind, out FileId info, uint size);
        [DllImport("kernel32.dll", EntryPoint="GetFileInformationByHandleEx", SetLastError=true, ExactSpelling=true)]
        [return: MarshalAs(UnmanagedType.Bool)]
        private static extern bool GetAttributeTag(SafeFileHandle handle, int kind, out AttributeTag info, uint size);
        [DllImport("kernel32.dll", SetLastError=true, ExactSpelling=true)]
        [return: MarshalAs(UnmanagedType.Bool)]
        private static extern bool FlushFileBuffers(SafeFileHandle handle);
        [DllImport("kernel32.dll", SetLastError=true, ExactSpelling=true)]
        [return: MarshalAs(UnmanagedType.Bool)]
        private static extern bool GetHandleInformation(SafeFileHandle handle, out uint flags);
        [DllImport("kernel32.dll", ExactSpelling=true)]
        private static extern IntPtr GetCurrentProcess();
        [DllImport("kernel32.dll", SetLastError=true, ExactSpelling=true)]
        [return: MarshalAs(UnmanagedType.Bool)]
        private static extern bool DuplicateHandle(IntPtr sourceProcess, SafeFileHandle source,
            IntPtr targetProcess, out SafeFileHandle target, uint access,
            [MarshalAs(UnmanagedType.Bool)] bool inherit, uint options);
        [DllImport("kernel32.dll", CharSet=CharSet.Unicode, SetLastError=true, ExactSpelling=true)]
        [return: MarshalAs(UnmanagedType.Bool)]
        private static extern bool CreateDirectoryW(string path, IntPtr security);
        [DllImport("kernel32.dll", CharSet=CharSet.Unicode, SetLastError=true, ExactSpelling=true)]
        [return: MarshalAs(UnmanagedType.Bool)]
        private static extern bool GetVolumeInformationW(string root, StringBuilder name, uint nameSize,
            out uint serial, out uint maximum, out uint flags, StringBuilder fs, uint fsSize);

        public static string Abi()
        {
            return String.Format("pointer={0};unicode={1};attributes={2};iosb={3};id={4};tag={5}",
                IntPtr.Size, Marshal.SizeOf<UnicodeString>(), Marshal.SizeOf<ObjectAttributes>(),
                Marshal.SizeOf<IoStatusBlock>(), Marshal.SizeOf<FileId>(), Marshal.SizeOf<AttributeTag>());
        }
        public static BoolResult CreateFixture(string path)
        {
            bool ok = CreateDirectoryW(path, IntPtr.Zero);
            int error = Marshal.GetLastWin32Error();
            return new BoolResult { Ok=ok, Error=ok ? 0 : error };
        }
        public static OpenResult OpenPath(string path, uint access, uint share, uint flags)
        {
            SafeFileHandle handle = CreateFileW(path, access, share, IntPtr.Zero, 3, flags, IntPtr.Zero);
            int error = Marshal.GetLastWin32Error();
            bool ok = handle != null && !handle.IsInvalid;
            return new OpenResult { Handle=handle, Api="CreateFileW", Access=access, Share=share,
                Options=flags, Disposition=3, Opened=ok, Error=ok ? 0 : error };
        }
        public static OpenResult CandidateA(SafeFileHandle original)
        {
            SafeFileHandle handle = ReOpenFile(original, Write, AllShare, Backup);
            int error = Marshal.GetLastWin32Error();
            bool ok = handle != null && !handle.IsInvalid;
            return new OpenResult { Handle=handle, Api="ReOpenFile_A", Access=Write,
                Share=AllShare, Options=Backup, Opened=ok, Error=ok ? 0 : error };
        }
        public static OpenResult EmptyNt(SafeFileHandle original, uint access)
        {
            bool retained = false;
            IntPtr unicode = IntPtr.Zero, empty = IntPtr.Zero, raw = IntPtr.Zero;
            try {
                original.DangerousAddRef(ref retained);
                empty = Marshal.AllocHGlobal(2);
                Marshal.WriteInt16(empty, 0);
                var name = new UnicodeString { Length=0, MaximumLength=0, Buffer=empty };
                unicode = Marshal.AllocHGlobal(Marshal.SizeOf<UnicodeString>());
                Marshal.StructureToPtr(name, unicode, false);
                var attributes = new ObjectAttributes {
                    Length=(uint)Marshal.SizeOf<ObjectAttributes>(), RootDirectory=original.DangerousGetHandle(),
                    ObjectName=unicode, Attributes=0, SecurityDescriptor=IntPtr.Zero,
                    SecurityQualityOfService=IntPtr.Zero
                };
                IoStatusBlock io = new IoStatusBlock { StatusOrPointer=IntPtr.Zero, Information=UIntPtr.Zero };
                int status = NtCreateFile(ref raw, access, ref attributes, ref io, IntPtr.Zero,
                    0, AllShare, 1, NtOptions, IntPtr.Zero, 0);
                // Preserve native return/IOSB before conversion, query, logging, or cleanup.
                string nt = "0x" + unchecked((uint)status).ToString("X8");
                string iosb = "0x" + unchecked((ulong)io.StatusOrPointer.ToInt64()).ToString("X16");
                string information = io.Information.ToUInt64().ToString();
                bool valid = raw != IntPtr.Zero && raw != new IntPtr(-1);
                bool ioComplete = io.StatusOrPointer == IntPtr.Zero;
                int mapped = status == 0 ? 0 : unchecked((int)RtlNtStatusToDosError(status));
                return new OpenResult {
                    Handle=new SafeFileHandle(raw, true), Api="NtCreateFile_empty_B",
                    Access=access, Share=AllShare, Options=NtOptions, Disposition=1,
                    Opened=status == 0 && valid && ioComplete, Error=mapped, NtStatus=nt, IoStatus=iosb,
                    IoInformation=information, UnexpectedHandle=(status != 0 || !ioComplete) && valid
                };
            } finally {
                if (unicode != IntPtr.Zero) Marshal.FreeHGlobal(unicode);
                if (empty != IntPtr.Zero) Marshal.FreeHGlobal(empty);
                if (retained) original.DangerousRelease();
            }
        }
        public static Identity Identify(SafeFileHandle handle)
        {
            FileId id;
            bool ok = GetFileInformationByHandleEx(handle, 18, out id, (uint)Marshal.SizeOf<FileId>());
            int error = Marshal.GetLastWin32Error();
            if (!ok) return new Identity { FailedQuery="FileIdInfo", Error=error };
            AttributeTag tag;
            ok = GetAttributeTag(handle, 9, out tag, (uint)Marshal.SizeOf<AttributeTag>());
            error = Marshal.GetLastWin32Error();
            if (!ok) return new Identity { FailedQuery="FileAttributeTagInfo", Error=error };
            return new Identity { Ok=true, Volume=id.Volume.ToString("X16"), Low=id.Low.ToString("X16"),
                High=id.High.ToString("X16"), Attributes=tag.Attributes, ReparseTag=tag.ReparseTag,
                Directory=(tag.Attributes & 0x10) != 0, Reparse=(tag.Attributes & 0x400) != 0 };
        }
        public static BoolResult Flush(SafeFileHandle handle)
        {
            bool ok = FlushFileBuffers(handle);
            int error = Marshal.GetLastWin32Error();
            return new BoolResult { Ok=ok, Error=ok ? 0 : error };
        }
        public static BoolResult HandleInfo(SafeFileHandle handle)
        {
            uint flags;
            bool ok = GetHandleInformation(handle, out flags);
            int error = Marshal.GetLastWin32Error();
            return new BoolResult { Ok=ok, Error=ok ? 0 : error, Flags=flags };
        }
        public static OpenResult Duplicate(SafeFileHandle original)
        {
            SafeFileHandle handle;
            IntPtr process = GetCurrentProcess();
            bool ok = DuplicateHandle(process, original, process, out handle, 0, false, 2);
            int error = Marshal.GetLastWin32Error();
            return new OpenResult { Handle=handle, Api="DuplicateHandle_same_access", Access=Read,
                Share=PrimaryShare, Options=2, Opened=ok && handle != null && !handle.IsInvalid,
                Error=ok ? 0 : error };
        }
        public static string FileSystem(string path)
        {
            uint serial, maximum, flags;
            var name = new StringBuilder(261);
            var fs = new StringBuilder(261);
            bool ok = GetVolumeInformationW(System.IO.Path.GetPathRoot(path), name, 261,
                out serial, out maximum, out flags, fs, 261);
            int error = Marshal.GetLastWin32Error();
            if (!ok) throw new Win32Exception(error, "GetVolumeInformationW");
            return fs.ToString();
        }
    }
}
'@
function Utc { [DateTime]::UtcNow.ToString('o') }
function Hash-Text([string] $Text) {
    [Convert]::ToHexString([Security.Cryptography.SHA256]::HashData(
        [Text.Encoding]::UTF8.GetBytes($Text))).ToLowerInvariant()
}
function Describe-Open($Open) {
    [ordered]@{
        api=$Open.Api; attempted=$true; acquired=$Open.Opened
        access=('0x{0:X8}' -f $Open.Access); share=('0x{0:X8}' -f $Open.Share)
        options=('0x{0:X8}' -f $Open.Options); disposition=$Open.Disposition
        win32_error=$Open.Error; nt_status=$Open.NtStatus; io_status_bits=$Open.IoStatus
        io_information=$Open.IoInformation; unexpected_handle=$Open.UnexpectedHandle
    }
}
function Describe-Identity($Identity) {
    [ordered]@{
        query_succeeded=$Identity.Ok; failed_query=$Identity.FailedQuery; win32_error=$Identity.Error
        volume=$Identity.Volume; low=$Identity.Low; high=$Identity.High
        attributes=('0x{0:X8}' -f $Identity.Attributes)
        reparse_tag=('0x{0:X8}' -f $Identity.ReparseTag)
        directory=$Identity.Directory; reparse=$Identity.Reparse
    }
}
function Assert-Identity($Identity, $Expected=$null) {
    if (-not $Identity.Ok) { throw "Identity query failed: $($Identity.FailedQuery), error=$($Identity.Error)" }
    if (-not $Identity.Directory -or $Identity.Reparse) { throw 'Not a plain non-reparse directory' }
    if ($null -ne $Expected -and (
        $Identity.Volume -ne $Expected.Volume -or $Identity.Low -ne $Expected.Low -or $Identity.High -ne $Expected.High)) {
        throw 'Native directory identity mismatch'
    }
}
function Initialize-Native {
    if (-not [Runtime.InteropServices.RuntimeInformation]::IsOSPlatform(
        [Runtime.InteropServices.OSPlatform]::Windows)) { throw 'Windows required; no fallback' }
    if ('HawdbDirectoryProbeV1.Native' -as [type]) { throw 'Use a fresh pwsh process; native type already loaded' }
    Add-Type -TypeDefinition $nativeSource -Language CSharp -ErrorAction Stop
    $abi=[HawdbDirectoryProbeV1.Native]::Abi()
    if ($abi -ne 'pointer=8;unicode=16;attributes=48;iosb=16;id=24;tag=8') { throw "Unsupported ABI: $abi" }
}
if ($Mode -eq 'Contender') {
    $handle=$null
    $code=1
    $receipt=[ordered]@{
        protocol='hawdb-directory-contender-v1'; fixture_tag=$FixtureTag; nonce=$AttemptNonce
        case=$AttemptCase; pid=$PID; started_utc=(Utc); script_sha256=$null
        attempted=$false; native=$null; identity=$null; harness_error=$null
    }
    try {
        if ([string]::IsNullOrWhiteSpace($FixturePath) -or $FixtureTag -notmatch '^[0-9a-f]{32}$' -or
            $AttemptNonce -notmatch '^[0-9a-f]{32}$' -or [string]::IsNullOrWhiteSpace($AttemptCase)) {
            throw 'Invalid contender parameters'
        }
        $receipt.script_sha256=(Get-FileHash -LiteralPath $PSCommandPath -Algorithm SHA256).Hash.ToLowerInvariant()
        if ($receipt.script_sha256 -ne $ExpectedScriptSha256) { throw 'Child source differs from parent' }
        Initialize-Native
        $opened=[HawdbDirectoryProbeV1.Native]::OpenPath($FixturePath, 0x80000000u, 6u, 0x02000000u)
        $handle=$opened.Handle
        $receipt.attempted=$true
        $receipt.native=Describe-Open $opened
        if ($opened.Opened) {
            $id=[HawdbDirectoryProbeV1.Native]::Identify($handle)
            $receipt.identity=Describe-Identity $id
            Assert-Identity $id
        }
        $code=0
    } catch { $receipt.harness_error=$_.Exception.Message }
    finally {
        if ($null -ne $handle -and -not $handle.IsClosed) { $handle.Dispose() }
        $receipt.ended_utc=(Utc)
        $receipt.handle_closed=($null -eq $handle -or $handle.IsClosed)
        [Console]::Out.WriteLine(($receipt | ConvertTo-Json -Depth 10 -Compress))
    }
    exit $code
}
if ([string]::IsNullOrWhiteSpace($OutputDirectory)) { throw 'Fresh OutputDirectory required' }
$OutputDirectory=[IO.Path]::GetFullPath($OutputDirectory)
if (Test-Path -LiteralPath $OutputDirectory) { throw 'Existing artifacts must not be overwritten' }
[IO.Directory]::CreateDirectory($OutputDirectory) | Out-Null
$utf8=[Text.UTF8Encoding]::new($false)
$script:Handles=[Collections.Generic.List[object]]::new()
$script:Cases=[Collections.Generic.List[object]]::new()
$script:ChildrenWaited=$true
$script:ChildCount=0
$script:RootId=$null
$script:Primary=$null
$script:Duplicate=$null
$script:Fixture=$null
$script:Created=$false
$plannedCases=@(
    'baseline_same_process','baseline_child','raw_reader_first','baseline_nt_read_write_diagnostic',
    'primary_acquire','primary_same_process_contender','primary_child_before_helpers',
    'current_read_write_diagnostic','attribute_equal_root','candidate_a','candidate_b',
    'primary_duplicate','original_primary_close','duplicate_only_child','final_primary_close',
    'released_same_process','released_child','cleanup'
)
$script:Summary=[ordered]@{
    protocol='hawdb-directory-custody-first-packet-v1'; expected_source_revision=$ExpectedSourceRevision
    source_revision=$null; script_sha256=(Get-FileHash -LiteralPath $PSCommandPath -Algorithm SHA256).Hash.ToLowerInvariant()
    native_source_sha256=Hash-Text $nativeSource; fixture_tag=[Guid]::NewGuid().ToString('N')
    started_utc=(Utc); ended_utc=$null; harness_outcome='running'; native_route_outcome='not_evaluated'
    candidate_a=$null; candidate_b=$null; planned_cases=$plannedCases; cases=$script:Cases
    child_budget_seconds=15; maximum_child_attempts=10; child_attempts=0
    harness_error=$null; cleanup='not_started'; sdk_qualification=$false
    cold_ro_qualified=$false; power_loss_qualified=$false
    broader_matrix='ACL/cursor/relative-child-IO/foreign/reparse/roles/coexistence not qualified'
}
function Save-Summary {
    $script:Summary.child_attempts=$script:ChildCount
    $temporary=Join-Path $OutputDirectory 'summary.json.pending'
    [IO.File]::WriteAllText($temporary, ($script:Summary | ConvertTo-Json -Depth 20), $utf8)
    [IO.File]::Move($temporary, (Join-Path $OutputDirectory 'summary.json'), $true)
}
function Event([string] $Name, $Details) {
    $row=[ordered]@{ time_utc=(Utc); event=$Name; details=$Details }
    [IO.File]::AppendAllText((Join-Path $OutputDirectory 'events.jsonl'),
        (($row | ConvertTo-Json -Depth 16 -Compress)+[Environment]::NewLine), $utf8)
    Save-Summary
}
function Begin-Case([string] $Name) {
    $row=[ordered]@{ case=$Name; state='started'; started_utc=(Utc); ended_utc=$null; details=$null }
    $script:Cases.Add($row)
    Event 'case_started' @{ case=$Name }
    return $row
}
function End-Case($Case, $Details) {
    $Case.details=$Details
    $Case.state='complete'
    $Case.ended_utc=(Utc)
    Event 'case_completed' @{ case=$Case.case; details=$Details }
}
function Keep($Open) {
    if ($null -ne $Open.Handle) { $script:Handles.Add($Open.Handle) }
    if ($Open.UnexpectedHandle -or $Open.NtStatus -eq '0x00000103' -or
        ($Open.NtStatus -eq '0x00000000' -and -not $Open.Opened)) {
        Event 'uncompleted_native_status' (Describe-Open $Open)
        throw 'Uncompleted/unexpected NtCreateFile handle or STATUS_PENDING'
    }
    return $Open.Handle
}
function Close-Handle($Handle) {
    if ($null -ne $Handle -and -not $Handle.IsClosed) { $Handle.Dispose() }
}
function Holder($Handle) {
    if ($null -eq $Handle -or $Handle.IsClosed -or $Handle.IsInvalid) { throw 'Expected holder is not live' }
    $info=[HawdbDirectoryProbeV1.Native]::HandleInfo($Handle)
    if (-not $info.Ok -or ($info.Flags -band 1u) -ne 0) {
        throw "Holder invalid/inheritable: error=$($info.Error), flags=$($info.Flags)"
    }
    return @{ live=$true; noninheritable=$true; flags=$info.Flags }
}
function Assert-Attempt($Receipt, [bool] $Acquire) {
    if (-not $Receipt.attempted -or $Receipt.native.api -ne 'CreateFileW' -or
        $Receipt.native.access -ne '0x80000000' -or $Receipt.native.share -ne '0x00000006' -or
        $Receipt.native.options -ne '0x02000000') { throw 'Missing/different actual contender attempt' }
    if ($Acquire) {
        $id=$Receipt.identity
        if (-not $Receipt.native.acquired -or $Receipt.native.win32_error -ne 0 -or
            $null -eq $id -or -not $id.query_succeeded -or -not $id.directory -or $id.reparse -or
            $id.volume -ne $script:RootId.Volume -or $id.low -ne $script:RootId.Low -or $id.high -ne $script:RootId.High) {
            throw 'Unobstructed positive did not acquire the same plain directory'
        }
    } elseif ($Receipt.native.acquired -or $Receipt.native.win32_error -ne 32) {
        throw "Expected actual sharing violation32; error=$($Receipt.native.win32_error)"
    }
}
function Same-Process([bool] $Acquire, $Held=$null) {
    $before=if ($null -ne $Held) { Holder $Held } else { $null }
    $opened=[HawdbDirectoryProbeV1.Native]::OpenPath($script:Fixture, 0x80000000u, 6u, 0x02000000u)
    $handle=Keep $opened
    try {
        $idReceipt=$null
        if ($opened.Opened) {
            $id=[HawdbDirectoryProbeV1.Native]::Identify($handle)
            Assert-Identity $id $script:RootId
            $idReceipt=Describe-Identity $id
        }
        $receipt=@{ attempted=$true; native=Describe-Open $opened; identity=$idReceipt }
        Assert-Attempt $receipt $Acquire
        return @{ receipt=$receipt; holder_before=$before; holder_after=if ($null -ne $Held) { Holder $Held } else { $null } }
    } finally { Close-Handle $handle }
}
function Child([string] $CaseName, [bool] $Acquire, $Held=$null) {
    $script:ChildCount++
    if ($script:ChildCount -gt 10) { throw 'Fixed child count exceeded' }
    $ordinal='{0:D2}' -f $script:ChildCount
    $before=if ($null -ne $Held) { Holder $Held } else { $null }
    $nonce=[Guid]::NewGuid().ToString('N')
    $info=[Diagnostics.ProcessStartInfo]::new()
    $info.FileName=(Get-Process -Id $PID).Path
    $info.UseShellExecute=$false
    $info.CreateNoWindow=$true
    $info.RedirectStandardOutput=$true
    $info.RedirectStandardError=$true
    foreach ($arg in @('-NoLogo','-NoProfile','-NonInteractive','-File',$PSCommandPath,
        '-Mode','Contender','-FixturePath',$script:Fixture,'-FixtureTag',$script:Summary.fixture_tag,
        '-AttemptNonce',$nonce,'-AttemptCase',$CaseName,'-ExpectedScriptSha256',$script:Summary.script_sha256)) {
        $info.ArgumentList.Add($arg)
    }
    $process=[Diagnostics.Process]::new()
    $process.StartInfo=$info
    $started=$false
    $waited=$false
    $outTask=$null
    $errTask=$null
    Event 'child_launch' @{ case=$CaseName; nonce=$nonce; ordinal=$ordinal; holder_before=$before }
    try {
        if (-not $process.Start()) { throw 'Contender launch failed' }
        $started=$true
        $outTask=$process.StandardOutput.ReadToEndAsync()
        $errTask=$process.StandardError.ReadToEndAsync()
        if (-not $process.WaitForExit(15000)) { throw 'Child15s timeout is incomplete, never sharing refusal' }
        $waited=$true
        if (-not $outTask.Wait(5000) -or -not $errTask.Wait(5000)) { throw 'Child output did not finish after exit' }
        $stdout=$outTask.GetAwaiter().GetResult()
        $stderr=$errTask.GetAwaiter().GetResult()
        $exit=$process.ExitCode
        $actualPid=$process.Id
        $after=if ($null -ne $Held) { Holder $Held } else { $null }
        Event 'child_waited' @{ case=$CaseName; pid=$actualPid; exit=$exit; holder_after=$after }
        if ($exit -ne 0 -or -not [string]::IsNullOrWhiteSpace($stderr)) { throw "Child harness failed, exit=$exit" }
        $receipt=$stdout.Trim() | ConvertFrom-Json -Depth 12
        if ($receipt.protocol -ne 'hawdb-directory-contender-v1' -or
            $receipt.fixture_tag -ne $script:Summary.fixture_tag -or $receipt.nonce -ne $nonce -or
            $receipt.case -ne $CaseName -or $receipt.pid -ne $actualPid -or
            $receipt.script_sha256 -ne $script:Summary.script_sha256 -or
            -not $receipt.handle_closed -or $null -ne $receipt.harness_error) { throw 'Unmatched actual child receipt' }
        Assert-Attempt $receipt $Acquire
        return @{ receipt=$receipt; actual_waited_exit=$exit; holder_before=$before; holder_after=$after }
    } finally {
        if ($started -and -not $waited) {
            try {
                if (-not $process.HasExited) { $process.Kill($true) }
                $waited=$process.WaitForExit(5000)
            } catch { $waited=$false }
            if (-not $waited) { $script:ChildrenWaited=$false }
            Event 'child_incomplete' @{ case=$CaseName; process_waited=$waited; sharing_refusal=$false }
        }
        if ($waited) {
            if ($null -ne $outTask -and $outTask.Wait(5000)) {
                [IO.File]::WriteAllText((Join-Path $OutputDirectory "child-$ordinal.stdout.txt"),
                    $outTask.GetAwaiter().GetResult(), $utf8)
            }
            if ($null -ne $errTask -and $errTask.Wait(5000)) {
                [IO.File]::WriteAllText((Join-Path $OutputDirectory "child-$ordinal.stderr.txt"),
                    $errTask.GetAwaiter().GetResult(), $utf8)
            }
        }
        $process.Dispose()
    }
}
function Candidate([string] $Name) {
    $case=Begin-Case "candidate_$Name"
    $before=Child "candidate_${Name}_before" $false $script:Primary
    Event 'candidate_open_started' @{ candidate=$Name; primary=Holder $script:Primary }
    $open=if ($Name -eq 'a') { [HawdbDirectoryProbeV1.Native]::CandidateA($script:Primary) }
        else { [HawdbDirectoryProbeV1.Native]::EmptyNt($script:Primary, 0x40100080u) }
    $handle=Keep $open
    $details=[ordered]@{
        candidate=$Name; before=$before; native=Describe-Open $open; identity=$null
        during='not_run_open_refused'; flush='not_run_open_refused'; helper_closed=$false
        after=$null; outcome='open_refused'; primitive_observed=$false
    }
    Event 'candidate_open_returned' $details
    try {
        if ($open.Opened) {
            $details.inheritance=Holder $handle
            $id=[HawdbDirectoryProbeV1.Native]::Identify($handle)
            $details.identity=Describe-Identity $id
            Event 'candidate_identity_returned' $details
            $helperBefore=Holder $handle
            $details.during=Child "candidate_${Name}_during" $false $script:Primary
            $details.during.helper_before=$helperBefore
            $details.during.helper_after=Holder $handle
            if (-not $id.Ok) {
                # No access widening and no Flush on an unbound object.
                $details.outcome='identity_query_refused'
                $details.flush='not_run_identity_unqualified'
            } else {
                Assert-Identity $id $script:RootId
                Event 'candidate_flush_started' @{ candidate=$Name; identity=Describe-Identity $id; primary=Holder $script:Primary }
                $flushed=[HawdbDirectoryProbeV1.Native]::Flush($handle)
                $details.flush=@{ success=$flushed.Ok; win32_error=$flushed.Error }
                $details.outcome=if ($flushed.Ok) { 'primitive_observed' } else { 'flush_refused' }
                $details.primitive_observed=$flushed.Ok
                Event 'candidate_flush_returned' $details
            }
        } elseif ($open.NtStatus -eq '0x00000000') { throw 'NT success with invalid handle' }
    } finally {
        Close-Handle $handle
        $details.helper_closed=($null -eq $handle -or $handle.IsClosed)
        Event 'candidate_helper_closed' @{ candidate=$Name; helper_closed=$details.helper_closed }
    }
    $details.after=Child "candidate_${Name}_after" $false $script:Primary
    End-Case $case $details
    return $details
}
$exitCode=1
try {
    Save-Summary
    [IO.File]::WriteAllText((Join-Path $OutputDirectory 'native-source.cs'), $nativeSource, $utf8)
    if ($ExpectedSourceRevision -notmatch '^[0-9a-fA-F]{40}$') { throw 'Exact40-character source revision required' }
    $revision=(& git -C $PSScriptRoot rev-parse HEAD).Trim()
    if ($LASTEXITCODE -ne 0 -or $revision -ne $ExpectedSourceRevision) { throw 'Checked-out source revision mismatch' }
    $script:Summary.source_revision=$revision
    Initialize-Native
    $environment=[ordered]@{
        os=[Environment]::OSVersion.VersionString
        architecture=[Runtime.InteropServices.RuntimeInformation]::ProcessArchitecture.ToString()
        os_architecture=[Runtime.InteropServices.RuntimeInformation]::OSArchitecture.ToString()
        framework=[Runtime.InteropServices.RuntimeInformation]::FrameworkDescription
        powershell=$PSVersionTable.PSVersion.ToString()
        compiler_assemblies=@([AppDomain]::CurrentDomain.GetAssemblies() |
            Where-Object { $_.GetName().Name -like 'Microsoft.CodeAnalysis*' } | ForEach-Object { $_.FullName })
        runner_image=$env:ImageOS; runner_image_version=$env:ImageVersion
        abi=[HawdbDirectoryProbeV1.Native]::Abi()
        masks=@{
            primary_access='0x80000000'; primary_share='0x00000006'; primary_options='0x02000000'
            a_access='0x40000000'; a_share='0x00000007'; a_options='0x02000000'
            b_access='0x40100080'; b_share='0x00000007'; b_disposition=1; b_options='0x00200021'
            b_object_attributes=0; b_backup_intent=$false; b_read_data=$false
            b_empty_name_length=0; b_empty_name_maximum=0
            attrs_access='0x00000080'; attrs_share='0x00000007'; attrs_options='0x02200000'
        }
    }
    [IO.File]::WriteAllText((Join-Path $OutputDirectory 'environment.json'), ($environment | ConvertTo-Json -Depth 10), $utf8)
    $script:Fixture=Join-Path ([IO.Path]::GetTempPath()) ("hawdb-dir-custody-"+$script:Summary.fixture_tag)
    $created=[HawdbDirectoryProbeV1.Native]::CreateFixture($script:Fixture)
    if (-not $created.Ok) { throw "Owned fixture creation failed, error=$($created.Error)" }
    $script:Created=$true
    $script:Summary.filesystem=[HawdbDirectoryProbeV1.Native]::FileSystem($script:Fixture)

    $case=Begin-Case 'baseline_same_process'
    $baseline=[HawdbDirectoryProbeV1.Native]::OpenPath($script:Fixture, 0x80000000u, 6u, 0x02000000u)
    $h=Keep $baseline
    try {
        if (-not $baseline.Opened) { throw "Unobstructed baseline failed, error=$($baseline.Error)" }
        $script:RootId=[HawdbDirectoryProbeV1.Native]::Identify($h)
        Assert-Identity $script:RootId
        End-Case $case @{ native=Describe-Open $baseline; identity=Describe-Identity $script:RootId; inheritance=Holder $h }
    } finally { Close-Handle $h }
    $case=Begin-Case 'baseline_child'
    End-Case $case (Child 'baseline_child' $true)

    $case=Begin-Case 'raw_reader_first'
    $raw=[HawdbDirectoryProbeV1.Native]::OpenPath($script:Fixture, 0x80000000u, 7u, 0x02000000u)
    $rawHandle=Keep $raw
    try {
        if (-not $raw.Opened) { throw 'Unobstructed raw reader failed' }
        Assert-Identity ([HawdbDirectoryProbeV1.Native]::Identify($rawHandle)) $script:RootId
        End-Case $case @{ raw=Describe-Open $raw; contender=Same-Process $false $rawHandle; adoption_approved=$false }
        $case=Begin-Case 'baseline_nt_read_write_diagnostic'
        $rwBaseline=[HawdbDirectoryProbeV1.Native]::EmptyNt($rawHandle, 0x40100081u)
        $rwHandle=Keep $rwBaseline
        try {
            if ($rwBaseline.Opened) { Assert-Identity ([HawdbDirectoryProbeV1.Native]::Identify($rwHandle)) $script:RootId }
            End-Case $case @{ native=Describe-Open $rwBaseline; supported_without_primary=$rwBaseline.Opened }
        } finally { Close-Handle $rwHandle }
    } finally { Close-Handle $rawHandle }

    $case=Begin-Case 'primary_acquire'
    $primaryOpen=[HawdbDirectoryProbeV1.Native]::OpenPath($script:Fixture, 0x80000000u, 6u, 0x02000000u)
    $script:Primary=Keep $primaryOpen
    if (-not $primaryOpen.Opened) { throw "Primary failed, error=$($primaryOpen.Error)" }
    Assert-Identity ([HawdbDirectoryProbeV1.Native]::Identify($script:Primary)) $script:RootId
    End-Case $case @{ native=Describe-Open $primaryOpen; inheritance=Holder $script:Primary }
    $case=Begin-Case 'primary_same_process_contender'
    End-Case $case (Same-Process $false $script:Primary)
    $case=Begin-Case 'primary_child_before_helpers'
    End-Case $case (Child 'primary_before_helpers' $false $script:Primary)
    $case=Begin-Case 'current_read_write_diagnostic'
    $rw=[HawdbDirectoryProbeV1.Native]::EmptyNt($script:Primary, 0x40100081u)
    $rwHandle=Keep $rw
    try {
        if ($rwBaseline.Opened -and ($rw.Opened -or $rw.Error -ne 32)) { throw 'Read+write diagnostic did not show sharing refusal after positive control' }
        End-Case $case @{ native=Describe-Open $rw; baseline=Describe-Open $rwBaseline; diagnostic_qualified=$rwBaseline.Opened }
    } finally { Close-Handle $rwHandle }

    $case=Begin-Case 'attribute_equal_root'
    $attrs=[HawdbDirectoryProbeV1.Native]::OpenPath($script:Fixture, 0x80u, 7u, 0x02200000u)
    $attrsHandle=Keep $attrs
    try {
        if (-not $attrs.Opened) { throw "Attribute-only observation failed, error=$($attrs.Error)" }
        $id=[HawdbDirectoryProbeV1.Native]::Identify($attrsHandle)
        Assert-Identity $id $script:RootId
        End-Case $case @{ native=Describe-Open $attrs; identity=Describe-Identity $id; current_replacement_qualified=$false }
    } finally { Close-Handle $attrsHandle }

    $script:Summary.candidate_a=Candidate 'a'
    $script:Summary.candidate_b=Candidate 'b'
    $case=Begin-Case 'primary_duplicate'
    $dup=[HawdbDirectoryProbeV1.Native]::Duplicate($script:Primary)
    $script:Duplicate=Keep $dup
    if (-not $dup.Opened) { throw "Duplicate failed, error=$($dup.Error)" }
    Assert-Identity ([HawdbDirectoryProbeV1.Native]::Identify($script:Duplicate)) $script:RootId
    End-Case $case @{ native=Describe-Open $dup; inheritance=Holder $script:Duplicate }
    $case=Begin-Case 'original_primary_close'
    Close-Handle $script:Primary
    End-Case $case @{ original_closed=$script:Primary.IsClosed; remaining_duplicate=Holder $script:Duplicate }
    $case=Begin-Case 'duplicate_only_child'
    End-Case $case (Child 'duplicate_only' $false $script:Duplicate)
    $case=Begin-Case 'final_primary_close'
    Close-Handle $script:Duplicate
    End-Case $case @{ original_closed=$script:Primary.IsClosed; duplicate_closed=$script:Duplicate.IsClosed }
    $case=Begin-Case 'released_same_process'
    End-Case $case (Same-Process $true)
    $case=Begin-Case 'released_child'
    End-Case $case (Child 'released_child' $true)

    $successful=@(@($script:Summary.candidate_a,$script:Summary.candidate_b) |
        Where-Object { $_.primitive_observed })
    $seen=@($script:Cases | ForEach-Object { $_.case })
    if (($seen -join '|') -cne ($plannedCases[0..16] -join '|') -or
        @($script:Cases | Where-Object { $_.state -ne 'complete' }).Count -ne 0) {
        throw 'Fixed case inventory incomplete or reordered'
    }
    $script:Summary.harness_outcome='completed'
    if ($successful.Count -gt 0) {
        $script:Summary.native_route_outcome='fixed_combination_observed_not_sdk_qualified'
        $exitCode=0
    } else {
        $script:Summary.native_route_outcome='both_fixed_candidates_falsified_or_unqualified'
        $exitCode=2
    }
} catch {
    $script:Summary.harness_outcome='failed'
    $script:Summary.harness_error=$_.Exception.Message
    Event 'harness_failed' @{ error=$_.Exception.Message }
    $exitCode=1
} finally {
    foreach ($handle in $script:Handles) { Close-Handle $handle }
    $case=Begin-Case 'cleanup'
    try {
        $closed=@($script:Handles | Where-Object { -not $_.IsClosed }).Count -eq 0
        if (-not $closed -or -not $script:ChildrenWaited) { throw 'Unsettled handles/child; fixture retained' }
        if ($script:Created) { [IO.Directory]::Delete($script:Fixture, $false) }
        $script:Summary.cleanup='completed_after_handles_children'
        End-Case $case @{ handles_closed=$closed; children_waited=$script:ChildrenWaited; recursive_delete=$false }
    } catch {
        $script:Summary.cleanup='failed_fixture_preserved'
        $script:Summary.harness_outcome='cleanup_failed'
        $script:Summary.cleanup_error=$_.Exception.Message
        End-Case $case @{ error=$_.Exception.Message; fixture_preserved=$true }
        $exitCode=1
    }
    $script:Summary.exit_code=$exitCode
    $script:Summary.ended_utc=(Utc)
    Save-Summary
    [Console]::Out.WriteLine(($script:Summary | ConvertTo-Json -Depth 20))
}
exit $exitCode
