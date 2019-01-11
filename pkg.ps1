push-location ./AzTableCopy
dotnet pack -c Release
Pop-Location

Push-Location ./AzTableCopy.Lib
dotnet pack -c Release
Pop-Location