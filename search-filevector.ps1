param(
  [string]$Root = 'A:\',
  [string]$OutFile = (Join-Path $PSScriptRoot 'filevector_files.txt'),
  [switch]$CaseSensitive
)

$pattern = 'FileVector\\w*'
$rg = Get-Command rg -ErrorAction SilentlyContinue

if ($rg) {
  $args = @('-l','--no-messages','--pcre2')
  if (-not $CaseSensitive) { $args += '-i' }
  $args += @('--glob','!**/proc/**','--glob','!**/sys/**','--glob','!**/dev/**','--glob','!**/lost+found/**')
  $args += @($pattern, $Root)

  & $rg @args |
    Sort-Object -Unique |
    Set-Content -Encoding UTF8 $OutFile
} else {
  $files = Get-ChildItem -Path $Root -File -Recurse -Force -ErrorAction SilentlyContinue |
    Where-Object { $_.FullName -notmatch '\\(proc|sys|dev|lost\+found)\\' }

  $files |
    Select-String -Pattern $pattern -CaseSensitive:$CaseSensitive -List |
    Select-Object -ExpandProperty Path |
    Sort-Object -Unique |
    Set-Content -Encoding UTF8 $OutFile
}

Write-Host "Wrote results to $OutFile"

