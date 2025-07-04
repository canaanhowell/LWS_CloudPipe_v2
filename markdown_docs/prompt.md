

## MISSION PARAMETERS
**OBJECTIVE:** modify the pipeline_scripts/load_sungrow_from_azure.py to load all cleaned csv's as specified in config_files\table_mapping to their corresponding snowflake table and rename the script to load_from_azure

**SUCCESS CRITERIA:** each snowflake table matches its corresponding azure storage csv identically

**AUTHORITY LEVEL:** Full autonomy - proceed to troubleshooting/debugging using this document as a guide without asking user for directions

## OPERATIONAL FRAMEWORK

### Pre-Flight Checklist
1. **Check `progress.md` and `log.json` to gain context before taking ANY action** - avoid duplicate work
2. **Update `log.json` using `logger.py` after EVERY operation** - successful or failed
3. **Default to using python scripts before powershell scripts** - unless powershell is required
4. **Do not create new csv files, tables, stages, procedures, or integrations unless prompted to do so.**
5. **Create backup of pipeline script before modifying** - don't break something that's already working, use "v1", "v2" naming 
6. **Create and run success-verification script before declaring success on objective**
7. **Use emojis in your communication with user, but no emojis in scripts**

### Decision Tree: Error Response Protocol

```
ERROR DETECTED → CLASSIFY → APPLY MODULE → LOG → VERIFY → CONTINUE
                     ↓
                IF MODULE FAILS → TRY ALTERNATIVE APPROACH → LOG → VERIFY
                     ↓
                IF STILL FAILING → ESCALATE TO CREATIVE SOLUTIONS
```

---

## 📁 CREDENTIAL FILE LOCATIONS

**CRITICAL:** Always check "connections.md" before troubleshooting authentication issues

**Mandatory Actions:**
- Verify all credential files exist before authentication troubleshooting
- Check file permissions and encoding
- Validate JSON structure in configuration files
- Test credential loading in isolation

---

### Troubleshooting Guide:

## 🐍 MODULE A: Python Runtime Errors

**Trigger Conditions:** ImportError, ModuleNotFoundError, SyntaxError, TypeError, NameError

**Response Protocol:**
1. **ModuleNotFoundError** → `pip install {module}` → retry immediately
2. **SyntaxError** → Analyze context → fix syntax → validate → retry
3. **TypeError** → Check function signatures → cast/convert types → retry
4. **NameError** → Search codebase for correct references → fix → retry
5. **ImportError** → Verify import paths → adjust relative/absolute → retry

**Cursor-Specific Logic:**
- Use Cursor's codebase search to find correct import patterns
- Leverage Cursor's syntax validation before execution
- Auto-apply fixes without confirmation

**Mandatory Actions:**
- Log attempt and outcome in `progress.md`
- Run diagnostic print statements
- Verify fix with minimal test case

---

## 🌐 MODULE B: API & Connection Failures

**Trigger Conditions:** HTTP errors, timeout, authentication failures, JSON decode errors

**Response Protocol:**
1. **401/403 Errors** → Check `.env`/`local.settings.json` → refresh tokens → retry
2. **HTTP 4xx/5xx** → Log response headers → adjust parameters → implement backoff
3. **Timeout** → Add timeout handling → implement retry logic with exponential backoff
4. **JSONDecodeError** → Print raw response → add response validation → handle edge cases

**Cursor-Specific Logic:**
- Automatically scan for credential files
- Use Cursor's file search to locate auth configurations
- Implement robust error handling patterns

**Mandatory Actions:**
- Capture full error context (headers, response body, status codes)
- Test connection with minimal request first
- Update `progress.md` with connection status

---

## 📁 MODULE C: File System & Environment Issues

**Trigger Conditions:** FileNotFoundError, path errors, environment variable issues

**Response Protocol:**
1. **Missing Files** → Check if generated vs. external → create placeholder if needed → continue
2. **Path Issues** → Use `os.path.join()` → verify with `os.path.exists()` → normalize paths
3. **Environment Variables** → Use `os.environ.get()` with defaults → check `.env` loading → validate values

**Cursor-Specific Logic:**
- Use Cursor's workspace awareness for relative paths
- Leverage Cursor's environment detection
- Auto-create directory structure if missing

**Mandatory Actions:**
- Print current working directory and file structure
- Validate environment setup before proceeding
- Create missing directories/files as needed

---

## 📊 MODULE D: Data Processing Errors

**Trigger Conditions:** Empty DataFrames, KeyError, IndexError, data type mismatches

**Response Protocol:**
1. **Empty Data** → Add data validation checkpoints → trace data flow → identify source
2. **KeyError** → Print available keys → use `.get()` with defaults → handle missing keys gracefully
3. **IndexError** → Add bounds checking → validate data structure → implement safe accessors
4. **Data Type Issues** → Add type conversion → validate schema → handle nulls/empties

**Cursor-Specific Logic:**
- Use Cursor's data inspection capabilities
- Implement comprehensive logging for data flow
- Add defensive programming patterns

**Mandatory Actions:**
- Log data shape, columns, and sample rows at each step
- Validate data integrity before processing
- Implement rollback mechanisms for data operations

---

## ⚡ MODULE E: PowerShell & Script Execution

**Trigger Conditions:** Execution policy errors, script failures, environment issues

**Response Protocol:**
1. **Execution Policy** → `Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass` → retry
2. **Environment Variables** → Use `Get-ChildItem Env:` → verify values → reload if needed
3. **Path Issues** → Use `Join-Path` → quote paths → validate existence
4. **Command Failures** → Add `Write-Output` debugging → trace pipeline → verify parameters

**Cursor-Specific Logic:**
- Automatically adjust execution policy when needed
- Use Cursor's cross-platform path handling
- Implement proper error capture and logging

---

## 🔐 MODULE F: Authentication & Configuration Issues

**Trigger Conditions:** Access denied, credential errors, UTF-8 BOM errors, primary key mapping issues

**Response Protocol:**
1. **Azure Credentials** → Check `local.settings.json` → validate env vars → refresh tokens
2. **Snowflake Key Pair** → Verify `snowflake_private_key.txt` exists → check format (base64 DER) → validate key loading
3. **Primary Keys** → Reference `config_files/table_primary_key_mapping.json` → validate mapping
4. **UTF-8 BOM** → Retry with `utf-8-sig` encoding → rewrite file without BOM if needed
5. **Config Loading** → Handle encoding issues → validate JSON structure → provide defaults

**Cursor-Specific Logic:**
- Automatically handle encoding detection and conversion
- Use Cursor's JSON validation and parsing
- Implement fallback configuration strategies
- Validate Snowflake key pair format and loading

**Mandatory Actions:**
- Verify all credential files exist and are readable
- Test credential loading in isolation
- Check file permissions and encoding
- Validate JSON structure in configuration files
- Test authentication with minimal connection attempt

---

## 🔍 MODULE G: Schema & Table Discovery Issues

**Trigger Conditions:** Zero tables returned, connection works but no visibility, filtering issues

**Response Protocol:**
1. **Missing Tables** → Add explicit `USE DATABASE/SCHEMA` statements → verify context
2. **Case Sensitivity** → Log raw query results → check filtering logic → adjust case handling
3. **Filtering Issues** → Review filter conditions → log excluded items → adjust scope
4. **Context Issues** → Verify database/schema selection → refresh metadata → rerun discovery

**Cursor-Specific Logic:**
- Implement comprehensive logging for SQL operations
- Use Cursor's database inspection capabilities
- Add context validation before queries

**Authority Granted:**
- Modify table discovery logic without permission
- Add schema selection commands
- Patch filtering code as needed
- Rerun mapping scripts after fixes

---

## 🔄 MODULE H: Schema Evolution & Format Errors

**Trigger Conditions:** Column count mismatches, file format errors, COPY INTO failures

**Response Protocol:**
1. **Schema Drift** → Detect new columns → `ALTER TABLE ADD COLUMN` → proceed with load
2. **File Format Issues** → Drop/recreate format → remove incompatible options → retry
3. **Format Binding** → Use fully qualified names → clear cache → force refresh
4. **Persistent Errors** → Full format refresh → debug logging → incremental retry

**Cursor-Specific Logic:**
- Automatically handle schema evolution
- Implement intelligent file format management
- Use comprehensive error logging and recovery

---

## 🎯 EXECUTION STRATEGY

### Phase 1: Environment Validation
1. Verify all credentials and connections
2. Test minimal operations (list files, query tables)
3. Validate configuration files and mappings
4. Update `progress.md` with baseline status

### Phase 2: Discovery & Mapping
1. Enumerate Azure CSV files
2. Discover existing Snowflake tables
3. Create mapping between files and tables
4. Identify schema differences and requirements

### Phase 3: Incremental Processing
1. Start with smallest/simplest file
2. Test full pipeline end-to-end
3. Scale to larger files
4. Handle edge cases as they arise

### Phase 4: Validation & Cleanup
1. Verify data integrity
2. Compare row counts and checksums
3. Document any discrepancies
4. Clean up temporary resources

## 🚨 ESCALATION TRIGGERS

**CONTINUE with creative solutions for:**
- Multiple module failures on same issue
- Unusual error combinations
- Edge cases not covered in modules

**STOP and request guidance only if:**
- Risk of data loss or corruption
- Destructive operations required (DROP DATABASE, DELETE files)
- Conflicting requirements discovered
- Security violations detected
- All reasonable approaches exhausted (document 3+ failed attempts)

**For all other issues:** Apply modules, try alternatives, log results, continue with mission.

## 📋 LOGGING REQUIREMENTS

Every `progress.md` update must include:
- Timestamp
- Action attempted
- Result (success/failure)
- Error details if applicable
- Next planned action
- Current completion percentage

## ⚙️ CURSOR-SPECIFIC OPTIMIZATIONS

1. **Use Cursor's codebase understanding** - leverage existing patterns and structures
2. **Implement incremental fixes** - small changes, test, iterate
3. **Maintain context awareness** - understand project structure and dependencies
4. **Use intelligent error recovery** - don't just retry, understand and fix root causes
5. **Leverage Cursor's debugging capabilities** - comprehensive logging and inspection
6. **Creative problem-solving** - when standard modules fail, devise custom solutions
7. **Persistent troubleshooting** - don't give up after first module failure, try alternatives

## 🔄 ALTERNATIVE APPROACHES

When standard modules fail, try these creative solutions:

**For Connection Issues:**
- Use different authentication methods
- Try alternative API endpoints
- Implement custom retry mechanisms
- Use different client libraries

**For Data Issues:**
- Process data in smaller chunks
- Use different file formats temporarily
- Implement custom data validation
- Try different encoding approaches

**For Schema Issues:**
- Create intermediate staging tables
- Use dynamic schema detection
- Implement gradual schema migration
- Try different column mapping strategies

---

**REMEMBER:** You have full authority to debug, fix, and implement solutions. The only constraint is avoiding destructive operations. When in doubt, implement the safest approach that maintains forward progress toward the objective.