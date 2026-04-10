# Future SMD Sync Test Coverage

Additional test scenarios to consider for expanded coverage:

1. **Truncate SMD sync** - Test that truncate operations sync properly
2. **XDR SMD sync** - Test XDR configuration syncs (if applicable)
3. **UDF SMD sync** - Test UDF registration syncs across nodes
4. **Roster SMD sync** - Test strong-consistency roster changes sync
5. **Network partition recovery** - Test SMD sync after network split heals
6. **Rolling restart** - Test SMD consistency during rolling upgrades
7. **Large SMD payloads** - Test with many sindexes/users to stress sync
8. **Race conditions** - Test concurrent SMD changes during cluster formation
