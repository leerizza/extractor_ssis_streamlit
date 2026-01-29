import streamlit as st
import xml.etree.ElementTree as ET
import pandas as pd
import re
from collections import defaultdict
import os
from quality_dashboard import render_quality_dashboard
from sql_refiner import SQLRefiner
from sql_parser import SQLParser

st.set_page_config(page_title="SSIS Metadata Extractor for Migration", layout="wide")

st.title("🔄 SSIS Package Metadata Extractor for Migration")
st.markdown("**Extract complete metadata from SSIS packages for migration purposes**")

class SSISMetadataExtractor:
    def __init__(self, xml_content):
        self.root = ET.fromstring(xml_content)
        self.namespaces = {
            'DTS': 'www.microsoft.com/SqlServer/Dts',
            'SQLTask': 'www.microsoft.com/sqlserver/dts/tasks/sqltask'
        }
        self.variable_map = self._cache_variables()
        self.conn_map = self._cache_connections()
        self.parser = SQLParser(variable_resolver=self._resolve_sql_variables)

    def _cache_connections(self):
        """Cache connection strings for quick lookup by ID and Name"""
        c_map = {}
        ns = '{www.microsoft.com/SqlServer/Dts}'
        
        for conn in self.root.findall('./DTS:ConnectionManagers/DTS:ConnectionManager', self.namespaces):
            conn_id = conn.get(f'{ns}DTSID')
            conn_name = conn.get(f'{ns}ObjectName')
            
            # Get connection string
            conn_string = ''
            conn_mgr = conn.find('.//DTS:ConnectionManager', self.namespaces)
            if conn_mgr is not None:
                conn_string = conn_mgr.get(f'{ns}ConnectionString', '')
                
            if conn_string:
                if conn_id:
                    c_map[conn_id] = conn_string
                if conn_name:
                    c_map[conn_name] = conn_string
                    c_map[f"Package.ConnectionManagers[{conn_name}]"] = conn_string
                    
        return c_map

    def _cache_variables(self):
        """Index variables for quick lookup"""
        v_map = {}
        ns = '{www.microsoft.com/SqlServer/Dts}'
        for var in self.root.findall('.//DTS:Variable', self.namespaces):
            name = var.get(f'{ns}ObjectName')
            val_elem = var.find('.//DTS:VariableValue', self.namespaces)
            val = val_elem.text if val_elem is not None else ''
            
            if name:
                v_map[name] = val
                v_map[f"User::{name}"] = val # Support qualified name
        return v_map

    def _resolve_sql_variables(self, sql_query):
        """Resolves SSIS variables (e.g. @[User::TableName]) in the SQL query"""
        if not sql_query or '@[' not in sql_query:
            return sql_query
            
        # Lazy load variables cache
        if not hasattr(self, '_variables_cache'):
            try:
                self._variables_cache = {
                    f"@[{v['Namespace']}::{v['Variable Name']}]": v['Value'] 
                    for v in self.get_variables()
                }
                # Also add short forms? SSIS usually mostly full Namespace
                # But let's add no-namespace version just in case of ambiguity?
                # Actually, standard is @[Namespace::Name]. Sometimes just ? for parameter.
                
                # Parameters handling (e.g. SQL Command from Variable) often just use the value.
                # But in "SQL Command" mode with variable, the whole string is the variable.
                # If "SQL Command" mode with text: "SELECT * FROM " + @[User::Table] -> Expression
                # Detailed parsing of expressions is hard, but simple string substitution works for direct embedding.
            except Exception:
                self._variables_cache = {}

        resolved_sql = sql_query
        
        # 1. Regex to find @[Namespace::Name] or @[Name]
        # Pattern: @\[([\w\s]+::[\w\s]+)\]
        pattern = r'@\[([\w\s:]+)\]'
        
        matches = re.findall(pattern, sql_query)
        for var_ref in matches:
            full_ref = f"@[{var_ref}]"
            # Try exact match first
            if full_ref in self._variables_cache:
                val = self._variables_cache[full_ref]
                # Determine if we should quote the value?
                # If table name, no quotes. If string literal, maybe quotes.
                # Heuristic: If it looks like a table name (no spaces) don't quote.
                # Better: Just substitute raw value and let parser succeed or fail.
                # usually variables for table names are raw names.
                resolved_sql = resolved_sql.replace(full_ref, val)
            else:
                # Try finding by name ignoring namespace if strict match failed
                # Not implemented for safety.
                pass
                
        return resolved_sql

        # Parsing logic moved to SQLParser class


    # Parsing logic moved to SQLParser class

    
    def get_package_info(self):
        """Extract basic package information"""
        ns = '{www.microsoft.com/SqlServer/Dts}'
        return {
            'Package Name': self.root.get(f'{ns}ObjectName', 'N/A'),
            'CreationDate': self.root.get(f'{ns}CreationDate', 'N/A'),
            'CreatorName': self.root.get(f'{ns}CreatorName', 'N/A'),
            'CreatorComputerName': self.root.get(f'{ns}CreatorComputerName', 'N/A'),
            'DTSID': self.root.get(f'{ns}DTSID', 'N/A'),
            'VersionBuild': self.root.get(f'{ns}VersionBuild', 'N/A'),
            'VersionMajor': self.root.get(f'{ns}VersionMajor', '0'),
            'VersionMinor': self.root.get(f'{ns}VersionMinor', '0'),
            # Legacy compatibility
            'Creator': self.root.get(f'{ns}CreatorName', 'N/A'),
            'Version Build': self.root.get(f'{ns}VersionBuild', 'N/A')
        }
    
    def get_connections(self):
        """Extract all connection managers"""
        connections = []
        ns = '{www.microsoft.com/SqlServer/Dts}'
        
        for conn in self.root.findall('./DTS:ConnectionManagers/DTS:ConnectionManager', self.namespaces):
            st.toast(f"Found CM: {conn.get(f'{ns}ObjectName')}")
            conn_name = conn.get(f'{ns}ObjectName')
            conn_type = conn.get(f'{ns}CreationName')
            conn_id = conn.get(f'{ns}DTSID')
            
            # Get connection string
            conn_string = ''
            server = 'N/A'
            database = 'N/A'
            
            conn_mgr = conn.find('.//DTS:ConnectionManager', self.namespaces)
            if conn_mgr is not None:
                conn_string = conn_mgr.get(f'{ns}ConnectionString', '')
                
                # Parse connection string
                if conn_string:
                    for part in conn_string.split(';'):
                        if '=' in part:
                            key, value = part.split('=', 1)
                            if key.strip() == 'Data Source':
                                server = value.strip()
                            elif key.strip() == 'Initial Catalog':
                                database = value.strip()
            
            connections.append({
                'Connection ID': conn_id,
                'Connection Name': conn_name,
                'Type': conn_type,
                'Server': server,
                'Database': database,
                'Full Connection String': conn_string
            })
        
        return connections
    
    def get_variables(self):
        """Extract all package variables"""
        variables = []
        ns = '{www.microsoft.com/SqlServer/Dts}'
        
        for var in self.root.findall('.//DTS:Variable', self.namespaces):
            var_name = var.get(f'{ns}ObjectName')
            var_namespace = var.get(f'{ns}Namespace', 'User')
            var_expression = var.get(f'{ns}Expression', '')
            
            var_value_elem = var.find('.//DTS:VariableValue', self.namespaces)
            var_value = var_value_elem.text if var_value_elem is not None else ''
            
            variables.append({
                'Namespace': var_namespace,
                'Variable Name': var_name,
                'Expression': var_expression,
                'Value': str(var_value)
            })
        
        return variables
    
    def get_executables(self):
        """Extract all executables (tasks)"""
        executables = []
        ns = '{www.microsoft.com/SqlServer/Dts}'
        
        for exe in self.root.findall('.//DTS:Executable', self.namespaces):
            exe_type = exe.get(f'{ns}ExecutableType', '')
            exe_name = exe.get(f'{ns}ObjectName', 'N/A')
            exe_desc = exe.get(f'{ns}Description', '')
            
            # Check if it's SQL Task
            sql_statement = 'N/A'
            if 'ExecuteSQLTask' in exe_type:
                sql_task = exe.find('.//SQLTask:SqlTaskData', self.namespaces)
                if sql_task is not None:
                    sql_source = sql_task.get('{www.microsoft.com/sqlserver/dts/tasks/sqltask}SqlStatementSource', '')
                    sql_statement = sql_source if sql_source else 'Variable/Expression'
            
            executables.append({
                'Task Name': exe_name,
                'Type': exe_type,
                'Description': exe_desc,
                'SQL Statement': sql_statement
            })
        
        return executables
    
    def _get_dataflow_task_name(self, component):
        """Helper to get the parent Data Flow Task name for a component"""
        # Traverse up to find the parent pipeline/dataflow element
        # Strategy: Find the nearest ancestor 'pipeline' element, then find its parent Executable
        
        # First, find all Data Flow Tasks
        ns = '{www.microsoft.com/SqlServer/Dts}'
        dataflow_tasks = {}
        
        for exe in self.root.findall('.//DTS:Executable', self.namespaces):
            exe_type = exe.get(f'{ns}ExecutableType', '')
            if 'Pipeline' in exe_type or 'DTS.Pipeline' in exe_type:
                task_name = exe.get(f'{ns}ObjectName', 'N/A')
                task_id = exe.get(f'{ns}DTSID', '')
                
                # Find the pipeline element within this executable
                pipeline = exe.find('.//pipeline', {})
                if pipeline is not None:
                    dataflow_tasks[id(pipeline)] = task_name
        
        # Now find which pipeline this component belongs to
        # Walk up from component to find pipeline ancestor
        current = component
        while current is not None:
            if current.tag == 'pipeline':
                return dataflow_tasks.get(id(current), 'Unknown Data Flow')
            current = self._get_parent(current)
        
        return 'Unknown Data Flow'
    
    def _get_parent(self, element):
        """Helper to get parent element (ElementTree doesn't have built-in parent)"""
        for parent in self.root.iter():
            for child in parent:
                if child == element:
                    return parent
        return None
    
    def get_dataflow_sources(self):
        """Extract all data sources from data flow tasks (Sources + Lookups)"""
        sources = []
        
        # Find all components with Source or Lookup in class ID
        for component in self.root.findall('.//component', {}):
            comp_class = component.get('componentClassID', '')
            
            # Treat Lookup as a Source (Reference Table)
            if 'Source' in comp_class or 'Lookup' in comp_class:
                comp_name = component.get('name', 'N/A')
                comp_desc = component.get('description', '')
                
                # Get connection
                connection_name = 'N/A'
                conn_elem = component.find('.//connection', {})
                if conn_elem is not None:
                    conn_ref = conn_elem.get('connectionManagerRefId', '')
                    if 'ConnectionManagers[' in conn_ref:
                        connection_name = conn_ref.split('[')[1].split(']')[0]
                
                # Get Properties (SQL, AccessMode, etc)
                sql_command = None
                table_name = None
                access_mode = 0
                sql_var = None
                column_to_table_map = {} # Reset map
                
                for prop in component.findall('.//property', {}):
                    prop_name = prop.get('name', '')
                    if prop_name == 'SqlCommand':
                        sql_command = prop.text
                    elif prop_name == 'OpenRowset':
                        table_name = prop.text
                    elif prop_name == 'AccessMode':
                        try:
                            access_mode = int(prop.text)
                        except:
                            pass
                    elif prop_name == 'SqlCommandVariable':
                        sql_var = prop.text
                
                # Handle SQL from Variable (AccessMode 3 usually)
                if access_mode == 3 and sql_var:
                    # Try to resolve variable
                    resolved_sql = self.variable_map.get(sql_var)
                    if not resolved_sql and '::' in sql_var:
                        # Try without namespace
                        resolved_sql = self.variable_map.get(sql_var.split('::')[-1])
                    
                    if resolved_sql:
                        sql_command = resolved_sql
                        comp_desc += f" (From Variable: {sql_var})"

                if sql_command:
                    # Use fresh parser to avoid cache pollution from complex queries
                    from sql_parser import SQLParser
                    local_parser = SQLParser(variable_resolver=self._resolve_sql_variables)
                    column_to_table_map = local_parser.parse_sql_column_sources(sql_command)

                
                # Get output columns
                output_columns = []
                for output in component.findall('.//output', {}):
                    output_name = output.get('name', '')
                    if 'Error' not in output_name:  # Skip error outputs
                        # Cache external metadata columns for name resolution
                        ext_meta_map = {} # id -> name
                        for ext in output.findall('.//externalMetadataColumn', {}):
                            ext_id = ext.get('refId') # Usually refId is used for linkage
                            # In Source components, outputColumn refers to externalMetadataColumnId matching refId?
                            # Actually usually matching 'id' of external col?
                            # Let's map both id and refId to name just in case
                            if ext.get('id'): ext_meta_map[ext.get('id')] = ext.get('name')
                            if ext.get('refId'): ext_meta_map[ext.get('refId')] = ext.get('name')

                        for col in output.findall('.//outputColumn', {}):
                            col_name = col.get('name', '')
                            
                            # Resolve REAL column name using External Metadata
                            # (Important if aliases are used in the component)
                            ext_ref = col.get('externalMetadataColumnId')
                            lookup_col_name = col_name
                            
                            # Check for Lookup Reference Column (Optimization)
                            # Lookup components map output to reference column via this property
                            copy_ref = None
                            for prop in col.findall('.//property', {}):
                                if prop.get('name') == 'CopyFromReferenceColumn':
                                    copy_ref = prop.text
                                    break
                            
                            if copy_ref:
                                lookup_col_name = copy_ref
                            elif ext_ref and ext_ref in ext_meta_map:
                                lookup_col_name = ext_meta_map[ext_ref]
                            
                            col_type = col.get('dataType', '')
                            col_length = col.get('length', '')
                            col_precision = col.get('precision', '')
                            col_scale = col.get('scale', '')
                            
                            col_def = f"{col_type}"
                            if col_length:
                                col_def += f"({col_length})"
                            elif col_precision:
                                col_def += f"({col_precision}"
                                if col_scale:
                                    col_def += f",{col_scale}"
                                col_def += ")"
                            
                            # Determine source table for this column
                            source_table = 'N/A'
                            source_col_original = lookup_col_name # Default to lookup name
                            expression = ''
                            
                            if column_to_table_map:
                                # Look up column in the mapping
                                mapped_data = column_to_table_map.get(lookup_col_name.upper())

                                if not mapped_data:
                                     mapped_data = column_to_table_map.get('*')
                                
                                if mapped_data:
                                    if isinstance(mapped_data, dict):
                                        source_table = mapped_data.get('source_table', 'N/A')
                                        source_col_original = mapped_data.get('source_column', lookup_col_name)
                                        expression = mapped_data.get('expression', '')
                                    else:
                                        source_table = str(mapped_data)
                                        source_col_original = 'N/A' 
                                        expression = ''
                                
                                # Fallback: Check for wildcard '*' mapping
                                if source_table == 'N/A' and '*' in column_to_table_map:
                                    wildcard_data = column_to_table_map['*']
                                    if isinstance(wildcard_data, dict):
                                        source_table = wildcard_data.get('source_table', 'N/A')
                                    else:
                                        source_table = str(wildcard_data)
                                    
                            elif table_name:
                                # If no SQL query, use the table name from OpenRowset
                                source_table = table_name
                            
                            elif 'FlatFileSource' in comp_class or 'ExcelSource' in comp_class:
                                source_table = connection_name
                                expression = 'File Read'

                            elif 'Lookup' in comp_class and not sql_command:
                                # Lookups might use Table Name property too?
                                # Usually 'SqlCommand' is set for query mode. 
                                # If 'NoCache' or 'FullCache' with table, might use OpenRowset too?
                                # Let's assume if table_name found it works.
                                pass

                            output_columns.append({
                                'Column Alias': col_name,
                                'Original Column': source_col_original,
                                'Source Table': source_table,
                                'Expression/Logic': expression,
                                'Data Type': col_def
                            })
                
                source_info = {
                    'Data Flow Task': self._get_dataflow_task_name(component),
                    'Component Name': comp_name,
                    'Component Type': comp_class,
                    'Connection': connection_name,
                    'Table/View': table_name if table_name else 'N/A',
                    'SQL Query': sql_command if sql_command else 'N/A',
                    'Description': comp_desc,
                    'Output Columns': output_columns
                }
                
                sources.append(source_info)
        
        return sources
    
    def get_dataflow_destinations(self):
        """Extract all destinations from data flow tasks"""
        destinations = []
        
        for component in self.root.findall('.//component', {}):
            comp_class = component.get('componentClassID', '')
            
            if 'Destination' in comp_class:
                comp_name = component.get('name', 'N/A')
                comp_desc = component.get('description', '')
                
                # Get connection
                connection_name = 'N/A'
                conn_elem = component.find('.//connection', {})
                if conn_elem is not None:
                    conn_ref = conn_elem.get('connectionManagerRefId', '')
                    if 'ConnectionManagers[' in conn_ref:
                        connection_name = conn_ref.split('[')[1].split(']')[0]
                
                # Get table name or SQL command
                table_name = 'N/A'
                sql_command = ''
                
                for prop in component.findall('.//property', {}):
                    prop_name = prop.get('name', '')
                    if prop_name == 'OpenRowset':
                        table_name = prop.text
                    elif prop_name == 'SqlCommand':
                        sql_command = prop.text
                
                # Fallback: If no table/SQL, check connection string (File Path)
                if table_name == 'N/A' and not sql_command:
                    conn_elem = component.find('.//connection', {})
                    if conn_elem is not None:
                        conn_ref = conn_elem.get('connectionManagerRefId', '')
                        if conn_ref and conn_ref in self.conn_map:
                             table_name = self.conn_map[conn_ref]
                
                # Get input columns (yang masuk ke destination)
                input_columns = []
                for input_elem in component.findall('.//input', {}):
                    input_name = input_elem.get('name', '')
                    if 'Error' not in input_name:
                        for col in input_elem.findall('.//inputColumn', {}):
                            col_name = col.get('cachedName', col.get('name', ''))
                            col_type = col.get('cachedDataType', '')
                            col_length = col.get('cachedLength', '')
                            
                            # Get external metadata (target column)
                            ext_meta_id = col.get('externalMetadataColumnId', '')
                            target_col_name = col_name  # Default sama
                            
                            if ext_meta_id:
                                # Cari external metadata column
                                for ext_col in input_elem.findall('.//externalMetadataColumn', {}):
                                    if ext_col.get('refId', '') == ext_meta_id:
                                        target_col_name = ext_col.get('name', col_name)
                                        break
                            
                            col_def = f"{col_type}"
                            if col_length:
                                col_def += f"({col_length})"
                            
                            input_columns.append({
                                'Source Column': col_name,
                                'Target Column': target_col_name,
                                'Data Type': col_def,
                                'Destination': comp_name
                            })
                
                dest_info = {
                    'Data Flow Task': self._get_dataflow_task_name(component),
                    'Component Name': comp_name,
                    'Component Type': comp_class,
                    'Connection': connection_name,
                    'Target Table': table_name,
                    'SQL Query': sql_command,
                    'Description': comp_desc,
                    'Input Columns': input_columns
                }
                
                destinations.append(dest_info)
        
        return destinations
    
    def get_transformations(self):
        """Extract all transformations"""
        transformations = []
        
        transform_classes = [
            'Microsoft.DerivedColumn',
            'Microsoft.MergeJoin',
            'Microsoft.Sort',
            'Microsoft.Lookup',
            'Microsoft.ConditionalSplit',
            'Microsoft.UnionAll',
            'Microsoft.DataConversion',
            'Microsoft.Aggregate'
        ]
        
        for component in self.root.findall('.//component', {}):
            comp_class = component.get('componentClassID', '')
            
            if any(tc in comp_class for tc in transform_classes):
                comp_name = component.get('name', 'N/A')
                comp_desc = component.get('description', '')
                
                details = {}
                
                # Derived Column - extract expressions
                if 'DerivedColumn' in comp_class:
                    expressions = []
                    for output in component.findall('.//output', {}):
                        for col in output.findall('.//outputColumn', {}):
                            col_name = col.get('name', '')
                            for prop in col.findall('.//property', {}):
                                if prop.get('name') == 'FriendlyExpression':
                                    expr = prop.text or ''
                                    expressions.append(f"{col_name} = {expr}")
                    details['Expressions'] = expressions
                
                # Merge Join - extract join type and keys
                elif 'MergeJoin' in comp_class:
                    for prop in component.findall('.//property', {}):
                        prop_name = prop.get('name', '')
                        if prop_name == 'JoinType':
                            join_type_map = {0: 'FULL', 1: 'LEFT', 2: 'INNER'}
                            details['Join Type'] = join_type_map.get(int(prop.text or 1), 'INNER')
                        elif prop_name == 'NumKeyColumns':
                            details['Key Columns'] = prop.text
                
                # Sort - extract sort columns
                elif 'Sort' in comp_class:
                    sort_cols = []
                    for input_elem in component.findall('.//input', {}):
                        for col in input_elem.findall('.//inputColumn', {}):
                            sort_pos = col.find('.//property[@name="NewSortKeyPosition"]', {})
                            if sort_pos is not None and sort_pos.text != '0':
                                col_name = col.get('cachedName', col.get('name', ''))
                                sort_cols.append(col_name)
                    details['Sort Columns'] = sort_cols
                
                transformations.append({
                    'Component Name': comp_name,
                    'Type': comp_class.replace('Microsoft.', ''),
                    'Description': comp_desc,
                    'Details': str(details)
                })
        
        return transformations

    def _get_dataflow_tasks(self):
        """Helper to find all Data Flow Task executables"""
        dfts = []
        ns = '{www.microsoft.com/SqlServer/Dts}'
        for exe in self.root.findall('.//DTS:Executable', self.namespaces):
            exe_type = exe.get(f'{ns}ExecutableType', '')
            if 'Pipeline' in exe_type or 'DTS.Pipeline' in exe_type:
                dfts.append(exe)
        return dfts

    def _trace_column_lineage_topology(self):
        """Holy Grail: Topological Tracing using LineageIDs (Multi-Source Capable)"""
        lineage_results = []
        
        # Process each Data Flow Task separately (LineageIDs are scoped to DFT)
        for dft in self._get_dataflow_tasks():
            pipeline = dft.find('.//pipeline', {})
            if pipeline is None: continue
            
            dft_name = dft.get('{www.microsoft.com/SqlServer/Dts}ObjectName', 'Unknown')
            
            # 1. Map Components and Paths
            # Use refId preferably (newer SSIS) or id (older)
            components = {}
            for c in pipeline.findall('.//component', {}):
                cid = c.get('refId') or c.get('id')
                if cid: components[cid] = c

            paths = pipeline.findall('.//path', {})
            
            # Graph: ComponentID -> [Downstream ComponentIDs]
            # Path maps OutputID (Start) -> InputID (End)
            # We need to map InputID -> ComponentID to traverse
            
            input_to_comp = {}
            for cid, comp in components.items():
                for inp in comp.findall('.//input', {}):
                    lid = inp.get('refId') or inp.get('id')
                    if lid: input_to_comp[lid] = cid
            
            adj_list = defaultdict(list)
            in_degree = defaultdict(int)
            
            # Initialize in-degree for all components
            for cid in components: in_degree[cid] = 0
                
            for path in paths:
                start_id = path.get('startId') # Output ID
                end_id = path.get('endId')     # Input ID
                
                # Find which component owns start_id? (Not strictly needed for adj)
                # Find which component owns end_id
                target_comp_id = input_to_comp.get(end_id)
                
                # We need source component ID.
                # OutputID -> ComponentID
                src_comp_id = None
                for cid, comp in components.items():
                    for out in comp.findall('.//output', {}):
                        oid = out.get('refId') or out.get('id')
                        if oid == start_id:
                            src_comp_id = cid
                            break
                    if src_comp_id: break
                
                if src_comp_id and target_comp_id:
                    adj_list[src_comp_id].append(target_comp_id)
                    in_degree[target_comp_id] += 1
            
            # 2. Lineage Map: LineageID -> LIST of SourceInfo Dicts
            # SourceInfo: {SourceComponent, SourceTable, OriginalColumn, Expression, ...}
            # List structure supports one column derived from multiple sources (1-to-Many Lineage)
            lineage_id_map = defaultdict(list)
            
            # 3. Topological Traversal (Queue based)
            queue = [cid for cid, deg in in_degree.items() if deg == 0]
            
            # Pre-calculate Source SQL info to avoid re-parsing
            # We can use our existing methods, filtering by component name or ID
            source_configs = self.get_dataflow_sources()
            source_config_map = {s['Component Name']: s for s in source_configs if s.get('Data Flow Task') == dft_name}
            
            processed_count = 0
            while queue:
                cid = queue.pop(0)
                comp = components[cid]
                processed_count += 1
                
                comp_class = comp.get('componentClassID', '')
                comp_name = comp.get('name', '')
                
                # --- PROCESS COMPONENT ---
                
                # A. Source Component / Lookup (Generator)
                if 'Source' in comp_class or 'Lookup' in comp_class:
                    if comp_name in source_config_map:
                        src_config = source_config_map[comp_name]
                        
                        # Map Outputs based on Alias (Name) match
                        # We need to find the LineageID for each output column
                        for output in comp.findall('.//output', {}):
                            for col in output.findall('.//outputColumn', {}):
                                lid = col.get('lineageId')
                                name = col.get('name')
                                
                                # Find matching config
                                col_config = next((c for c in src_config['Output Columns'] if c['Column Alias'] == name), None)
                                
                                if col_config and lid:
                                    # Initialize as LIST with one source
                                    lineage_id_map[lid] = [{
                                        'Source Component': comp_name,
                                        'Source Table': col_config['Source Table'],
                                        'Original Column': col_config['Original Column'],
                                        'Expression/Logic': col_config['Expression/Logic'],
                                        'Source Type': col_config['Data Type']
                                    }]
                
                # B. Transformations (Pass-through vs Async)
                else:
                    # Logic: 
                    # 1. Identify Input Lines (Upstream Sources)
                    # 2. Identify Output Columns
                    #    - If Synchronous (share LineageID with Input), inherit Source info.
                    #    - If Asynchronous (New LineageID), try to map input -> output.
                    
                    # Map: InputLineageID -> List[UpstreamInfo]
                    # We can access lineage_id_map directly.
                    
                    # Store Input Columns metadata for Expression lookup
                    # Map: Name -> LineageID
                    input_name_map = {}
                    for inp in comp.findall('.//input', {}):
                        for col in inp.findall('.//inputColumn', {}):
                            lid = col.get('lineageId')
                            # Prefer 'name' (Source Name) or 'cachedName' (Input Name)?
                            # SSIS Expressions usually reference the Input Column Name.
                            cname = col.get('name')
                            if cname: input_name_map[cname] = lid
                            cached_name = col.get('cachedName')
                            if cached_name: input_name_map[cached_name] = lid

                    for output in comp.findall('.//output', {}):
                        sync_id = output.get('synchronousInputId') # If set, this is synchronous
                        
                        for out_col in output.findall('.//outputColumn', {}):
                            lid = out_col.get('lineageId')
                            if not lid: continue
                            
                            # Case 1: Existing LineageID (Pass-through) - ALREADY HANDLED by Python Ref logic?
                            # No, lineage_id_map persists. 
                            # If pass-through, no action needed unless we want to tag "Passed through X".
                            
                            # Case 2: New Lineage ID (Derived or Async)
                            if lid in lineage_id_map:
                                # Start with existing info (flow through)
                                # Logic to update expression?
                                pass
                            else:
                                # New ID. Need to derive source.
                                new_sources = []
                                
                                if 'UnionAll' in comp_class:
                                    # Union Logic (Match by Index)
                                    out_cols = list(output.findall('.//outputColumn', {}))
                                    try:
                                        idx = out_cols.index(out_col)
                                        used_source_keys = set()
                                        
                                        for inp in comp.findall('.//input', {}):
                                            in_cols = list(inp.findall('.//inputColumn', {}))
                                            if idx < len(in_cols):
                                                in_lid = in_cols[idx].get('lineageId')
                                                if in_lid in lineage_id_map:
                                                    upstream_list = lineage_id_map[in_lid]
                                                    for src in upstream_list:
                                                        # Deduplicate by Source Table + Col
                                                        key = (src['Source Table'], src['Original Column'])
                                                        if key not in used_source_keys:
                                                            used_source_keys.add(key)
                                                            new_src = src.copy()
                                                            new_src['Expression/Logic'] += f" -> Union({comp_name})"
                                                            new_sources.append(new_src)
                                    except:
                                        pass
                                


                                elif 'DataConvert' in comp_class:
                                    # Data Conversion: Map Output -> Input via SourceInputColumnLineageID
                                    source_lid_prop = None
                                    for prop in out_col.findall('.//property', {}):
                                        if prop.get('name') == 'SourceInputColumnLineageID':
                                            source_lid_prop = prop.text
                                            if source_lid_prop:
                                                 # Strip #{ } wrapper if present
                                                 source_lid_prop = source_lid_prop.strip().replace('#{', '').replace('}', '')
                                                 # print(f"DEBUG: DataConv Key: {source_lid_prop} Found? {source_lid_prop in lineage_id_map}")
                                            break
                                    
                                    if source_lid_prop and source_lid_prop in lineage_id_map:
                                        upstream_list = lineage_id_map[source_lid_prop]
                                        for src in upstream_list:
                                            new_src = src.copy()
                                            new_src['Expression/Logic'] += f" -> Conv({out_col.get('dataType')})"
                                            new_sources.append(new_src)

                                elif 'MergeJoin' in comp_class or 'Sort' in comp_class or 'Aggregate' in comp_class:
                                    # Async Pass-Through by Name
                                    name = out_col.get('name')
                                    src_lid = input_name_map.get(name)
                                    
                                    if not src_lid:
                                        # SSIS often uses cachedName in MergeJoin/Sort
                                        # Or internal lineage ID mappings? 
                                        # For now, try case-insensitive and cachedName lookup
                                        src_lid = next((v for k,v in input_name_map.items() if k.upper() == (name or '').upper()), None)
                                    
                                    if src_lid and src_lid in lineage_id_map:
                                        upstream_list = lineage_id_map[src_lid]
                                        for src in upstream_list:
                                            new_src = src.copy()
                                            # We generally just pass through, but can tag if needed
                                            # new_src['Expression/Logic'] += f" -> {comp_name}"
                                            new_sources.append(new_src)
                                    else:
                                        # Fallback
                                        new_sources.append({
                                            'Source Component': comp_name,
                                            'Source Table': 'Transformation',
                                            'Original Column': name,
                                            'Expression/Logic': f'Async ({comp_name})',
                                            'Source Type': out_col.get('dataType')
                                        })

                                elif 'DerivedColumn' in comp_class:
                                    # Expression Parsing Logic
                                    expr = ''
                                    for prop in out_col.findall('.//property', {}):
                                        if prop.get('name') == 'FriendlyExpression':
                                            expr = prop.text
                                    
                                    if expr:
                                        # Parse dependencies: [ColName] or ColName
                                        # SSIS Expressions usually use brackets, but some (like C# style) might not?
                                        # Enhanced Regex: Capture [Brackets] OR \bWords\b
                                        # Filter out keywords/literals later
                                        raw_deps = re.findall(r'\[(.*?)\]|\b([a-zA-Z_][\w]*)\b', expr)
                                        
                                        deps = []
                                        for m in raw_deps:
                                            # m is tuple (bracketed, unbracketed)
                                            val = m[0] if m[0] else m[1]
                                            # Filter keywords/literals
                                            if val and not val.startswith('"') and not val.isnumeric() and val.upper() not in ['TRUE', 'FALSE', 'NULL', 'ISNULL', 'TRIM', 'LEN', 'SUBSTRING', 'GETDATE', 'DATEADD', 'DATEDIFF', 'DT_STR', 'DT_WSTR', 'DT_DBTIMESTAMP', 'DT_I4', 'DT_R8']:
                                                deps.append(val)
                                        
                                        # Filter out variables (User::...)
                                        col_deps = [d for d in deps if '::' not in d]
                                        
                                        if not col_deps and deps:
                                            # Depends only on variables?
                                            new_sources.append({
                                                'Source Component': comp_name,
                                                'Source Table': 'Variable/Expression',
                                                'Original Column': 'Expression',
                                                'Expression/Logic': expr,
                                                'Source Type': 'Derived'
                                            })
                                        
                                        used_source_keys = set()
                                        for d in col_deps:
                                            # Find input LineageID
                                            # Try direct name match first
                                            in_lid = input_name_map.get(d)
                                            # If not found, try case-insensitive?
                                            if not in_lid:
                                                 in_lid = next((v for k,v in input_name_map.items() if k.upper() == d.upper()), None)

                                            if in_lid and in_lid in lineage_id_map:
                                                upstream_list = lineage_id_map[in_lid]
                                                for src in upstream_list:
                                                    key = (src['Source Table'], src['Original Column'])
                                                    # Allow multiple columns if different tables?
                                                    # Actually, allows same table different cols (ColA + ColB).
                                                    # Key should include d (the input column name used).
                                                    # Let's just append everything.
                                                    new_src = src.copy()
                                                    new_src['Expression/Logic'] = f"{src['Expression/Logic']} -> Derived({d})"
                                                    new_sources.append(new_src)
                                    
                                    if not new_sources:
                                        # Fallback if no deps found or parse failed
                                        new_sources.append({
                                            'Source Component': comp_name,
                                            'Source Table': 'Derived / Transformation',
                                            'Original Column': out_col.get('name'),
                                            'Expression/Logic': expr or 'Derived',
                                            'Source Type': out_col.get('dataType')
                                        })
                                
                                else:
                                    # Unknown origin (Async transform?)
                                    new_sources.append({
                                        'Source Component': comp_name,
                                        'Source Table': 'Transformation',
                                        'Original Column': out_col.get('name'),
                                        'Expression/Logic': 'Unknown Logic',
                                        'Source Type': out_col.get('dataType')
                                    })
                                
                                # Assign list
                                lineage_id_map[lid] = new_sources

                # C. Destination Component (Consumer)
                if 'Destination' in comp_class:
                     # Get Target Table info
                     target_table = 'N/A'
                     for prop in comp.findall('.//property', {}):
                         if prop.get('name') == 'OpenRowset': target_table = prop.text
                     
                     if target_table == 'N/A':
                         conn_elem = comp.find('.//connection', {})
                         if conn_elem is not None:
                             cm_ref = conn_elem.get('connectionManagerRefId')
                             if cm_ref and cm_ref in self.conn_map:
                                 target_table = self.conn_map[cm_ref]
                     
                     # Map Inputs
                     for inp in comp.findall('.//input', {}):
                         for in_col in inp.findall('.//inputColumn', {}):
                             lid = in_col.get('lineageId')
                             target_col = in_col.get('cachedName', in_col.get('name')) # Destination Col Name
                             
                             # Resolve External Metadata if available (to get real Target Column)
                             ext_id = in_col.get('externalMetadataColumnId')
                             if ext_id:
                                 for ext in inp.findall('.//externalMetadataColumn', {}):
                                     if ext.get('refId') == ext_id:
                                         target_col = ext.get('name')
                                         break
                             
                             if lid in lineage_id_map:
                                 src_infos = lineage_id_map[lid] # LIST of sources
                                 for src_info in src_infos:
                                     lineage_results.append({
                                         'Source Component': src_info['Source Component'],
                                         'Source Table': src_info['Source Table'],
                                         'Original Column': src_info['Original Column'],
                                         'Expression/Logic': src_info.get('Expression/Logic', ''),
                                         'Source Column': src_info['Original Column'], # Tracked origin name
                                         'Source Type': src_info.get('Source Type', ''),
                                         'Destination Component': comp_name,
                                         'Destination Table': target_table,
                                         'Destination Column': target_col,
                                         'Destination Type': in_col.get('cachedDataType', '')
                                     })
                             elif target_col:
                                 # FALLBACK: Try to match by Name if LineageID missing
                                 # Useful for stale packages where IDs are broken but names match
                                 # We search upstream components in the same Data Flow
                                 # Find upstream component
                                 # We need to know which component feeds this input.
                                 
                                 # Reverse look up adjacency?
                                 # input_to_comp maps InputID -> ComponentID.
                                 # But we need to know who feeds this InputID.
                                 # paths maps startId (Output) -> endId (Input)
                                 
                                 # 1. Find the path ending at this input's ID (or the Input ID itself)
                                 input_id = inp.get('refId') or inp.get('id')
                                 upstream_output_id = None
                                 for path in paths:
                                     if path.get('endId') == input_id:
                                         upstream_output_id = path.get('startId')
                                         break
                                 
                                 if upstream_output_id:
                                      # Find identifying component
                                      upstream_cid = None
                                      for c_id, c_comp in components.items():
                                          for out in c_comp.findall('.//output', {}):
                                              if (out.get('refId') or out.get('id')) == upstream_output_id:
                                                  upstream_cid = c_id
                                                  break
                                          if upstream_cid: break
                                      
                                      if upstream_cid:
                                          # We found the upstream component.
                                          # Does it have an output column with this name?
                                          up_comp = components[upstream_cid]
                                          up_comp_name = up_comp.get('name')
                                          
                                          # Check source config map if it's a source
                                          if up_comp_name in source_config_map:
                                               src_cfg = source_config_map[up_comp_name]
                                               # Match by Output Column Name ~ Target Col Name
                                               match_col = next((c for c in src_cfg['Output Columns'] if c['Column Alias'].upper() == target_col.upper()), None)
                                               
                                               if match_col:
                                                   lineage_results.append({
                                                       'Source Component': up_comp_name,
                                                       'Source Table': match_col['Source Table'],
                                                       'Original Column': match_col['Original Column'],
                                                       'Expression/Logic': match_col['Expression/Logic'] + ' (Name Match)',
                                                       'Source Column': match_col['Original Column'],
                                                       'Source Type': match_col['Data Type'],
                                                       'Destination Component': comp_name,
                                                       'Destination Table': target_table,
                                                       'Destination Column': target_col,
                                                       'Destination Type': in_col.get('cachedDataType', '')
                                                   })
                                               # Synthetic Fallback for Stale Table Sources
                                               # If matching failed, but Source is a Table (not Query), assume it exists.
                                               elif src_cfg.get('Table/View') and src_cfg['Table/View'] != 'N/A' and src_cfg['SQL Query'] == 'N/A':
                                                    lineage_results.append({
                                                       'Source Component': up_comp_name,
                                                       'Source Table': src_cfg['Table/View'],
                                                       'Original Column': target_col, # Assume same name
                                                       'Expression/Logic': 'Inferred (Stale Package)',
                                                       'Source Column': target_col,
                                                       'Source Type': 'Inferred',
                                                       'Destination Component': comp_name,
                                                       'Destination Table': target_table,
                                                       'Destination Column': target_col,
                                                       'Destination Type': in_col.get('cachedDataType', '')
                                                   })
                                          # Check if it has lineage info mapped by Name?
                                          # Complex recursive check omitted for brevity,
                                          # tackling primary use case: Stale Source -> Destination.

            
                # Push downstream
                for neighbor in adj_list[cid]:
                    in_degree[neighbor] -= 1
                    if in_degree[neighbor] == 0:
                        queue.append(neighbor)
                        
        return lineage_results

    def get_column_lineage(self):
        """Extract complete column lineage using Topological LineageID Tracing"""
        try:
            return self._trace_column_lineage_topology()
        except Exception as e:
            print(f"Graph Lineage Failed: {e}")
            return []

    def get_unused_columns(self):
        """Identify columns from Sources that are NOT used in any Destination"""
        unused_report = []
        
        # 1. Get all available source columns
        sources = self.get_dataflow_sources()
        
        # 2. Get all used lineage
        lineage = self.get_column_lineage()
        
        # Map Source Component -> Set of Used Columns
        used_cols_map = defaultdict(set)
        for l in lineage:
            src_comp = l.get('Source Component')
            # Use 'Source Column' (which maps to Original Column usually)
            # But wait, source['Output Columns'] has matches.
            # In _trace_column_lineage_topology:
            # 'Source Table': col_config['Source Table'],
            # 'Original Column': col_config['Original Column'],
            # The 'Original Column' is what comes from SQL Parser.
            
            # Let's check what source['Output Columns'] contains.
            # It contains 'Column Alias', 'Source Column', 'Original Column'.
            # 'Column Alias' is the name in the data flow.
            # Lineage tracking traces LineageIDs.
            
            # Key to match: The 'Column Alias' from Source Config VS... wait.
            # In trace topology: 
            # col_config = next((c for c in src_config['Output Columns'] if c['Column Alias'] == name), None)
            
            # So if a column is in lineage, it was matched by 'Column Alias' (name).
            # We can just track which LineageIDs from Source were visited?
            # But LineageIDs are internal.
            
            # Simpler: Trace by Source Component + Column Alias.
            # The lineage result doesn't explicitly store 'Column Alias' of the source output, 
            # but it stores 'Original Column' (from SQL).
            
            # However, if we want to flag "Unused Columns", we usually mean "Columns in SELECT list".
            # The 'Output Columns' list IS the SELECT list (parsed).
            
            # So we need to know which entries in 'Output Columns' were involved in lineage.
            # Re-running trace might be expensive.
            
            # Strategy:
            # In trace topology, we iterate sources and find matching col_config. 
            # If we just checked which col_configs were accessed, we'd know.
            
            # Post-processing Strategy:
            # Lineage items have 'Source Component', 'Source Table', 'Original Column'.
            # Source items have 'Component Name', 'Output Columns' -> [{'Column Alias', 'Source Table', 'Original Column'}]
            
            # We can match on (SourceTable, OriginalColumn) tuple? 
            # Or just (Original Column) if distinct enough per component.
            
            if src_comp:
                orig_col = l.get('Source Column') # This is actually 'Original Column' in lineage dict key
                if orig_col:
                    used_cols_map[src_comp].add(orig_col.upper())
                    
        for val in sources:
            comp_name = val['Component Name']
            used_set = used_cols_map.get(comp_name, set())
            
            unused_in_source = []
            for col in val['Output Columns']:
                # What is the unique identifier? 'Original Column' or 'Column Alias'?
                # 'Original Column' comes from SQL logic.
                # 'Column Alias' is the OutputColumn name. 
                # If 'Column Alias' differs, lineage trace used 'Column Alias' to find the config, 
                # then stamped 'Original Column' into the lineage result.
                
                # So if we have 'Original Column' in lineage, we match against 'Original Column' here.
                orig = col.get('Original Column')
                if orig and orig != 'N/A':
                    if orig.upper() not in used_set:
                        # Double check if it's expression/calculated?
                        src_tbl = col.get('Source Table')
                        if src_tbl not in ['Expression/Literal', 'Expression', 'Literal', 'Static Value', 'Calculation']:
                             unused_in_source.append(orig)
            
            if unused_in_source:
                unused_report.append({
                    'Source Component': comp_name,
                    'Unused Count': len(unused_in_source),
                    'Unused Columns': ", ".join(sorted(unused_in_source))
                })
                
        return unused_report

    def refine_package_sql(self):
        """
        Refines SQL commands in the package using SQLRefiner.
        Returns:
            list: List of changes (dict with Component, OldSQL, NewSQL)
        """
        changes = []
        refiner = SQLRefiner()
        
        # Iterate over all components (Data Flow Components)
        # We need to look for 'SqlCommand' property in components
        for pipeline in self.root.findall('.//DTS:Executable', self.namespaces):
            # Check if it's a Data Flow Task
            if 'Pipeline' in pipeline.get(f'{{{self.namespaces["DTS"]}}}CreationName', ''):
                obj_data = pipeline.find('.//DTS:ObjectData', self.namespaces)
                if obj_data:
                    pipeline_xml = obj_data.find('.//pipeline', self.namespaces) # Note: pipeline has no namespace prefix usually or different one?
                    # Actually valid pipeline XML inside ObjectData uses generic 'pipeline' tag or defaults.
                    # Let's try finding all components recursively from root might be easier if we just want to patch properties.
                    pass 

        # Global search for properties with Name='SqlCommand'
        # This is safer as it covers all components
        for prop in self.root.findall('.//property[@name="SqlCommand"]', {}): # property tag might not have namespace
             # Wait, elementtree findall with empty namespace dict might miss if they have namespace.
             # In .dtsx, <property> is usually under <component>.
             # Let's verify the XML structure from previous file reads.
             # Usually: <component ...> <properties> <property name="SqlCommand">...</property>
             pass

        # Let's use the iterator we used in get_dataflow_sources but for patching
        # Re-implementing simplified traversal
        dataflow_tasks = self._get_dataflow_tasks()
        
        for task in dataflow_tasks:
            obj_data = task.find('.//DTS:ObjectData', self.namespaces)
            if not obj_data: continue
            
            pipeline_inner = obj_data.find('.//pipeline') # Usually no namespace for inner pipeline
            if pipeline_inner is None: continue
            
            for component in pipeline_inner.findall('.//component'):
                comp_name = component.get('name')
                
                # specific check for SqlCommand property
                for prop in component.findall('.//property'):
                    if prop.get('name') == 'SqlCommand':
                        original_sql = prop.text
                        if original_sql:
                            refined_sql = refiner.refine(original_sql)
                            
                            # Normalize line endings for comparison
                            if original_sql.strip() != refined_sql.strip():
                                prop.text = refined_sql
                                changes.append({
                                    "component": comp_name,
                                    "old": original_sql,
                                    "new": refined_sql
                                })
        
        return changes

# File uploader

# @st.cache_data(show_spinner=False)
def process_package_metadata(xml_content):
    """
    Process package content and return all metadata.
    Cached to prevent re-processing on re-runs.
    """
    extractor = SSISMetadataExtractor(xml_content)
    
    return {
        'info': extractor.get_package_info(),
        'connections': extractor.get_connections(),
        'variables': extractor.get_variables(),
        'executables': extractor.get_executables(),
        'sources': extractor.get_dataflow_sources(),
        'destinations': extractor.get_dataflow_destinations(),
        'transformations': extractor.get_transformations(),
        'lineage': extractor.get_column_lineage(),
        'unused': extractor.get_unused_columns()
    }

def render_sql_script_analyzer(package_name="Global"):
    """
    Renders the SQL Script Analyzer UI (SPs and Views).
    Can be used as a standalone tool or as a tab within a package.
    """
    st.subheader("📜 SQL Script Analyzer")
    
    # Split into sub-tabs as requested
    sub_tab_sp, sub_tab_view = st.tabs(["Stored Procedures", "Views"])

    # --- Sub-Tab 1: Stored Procedures ---
    with sub_tab_sp:
        st.markdown("Paste your `CREATE PROCEDURE` script here to reverse-engineer its lineage.")
        
        sp_content = st.text_area("Stored Procedure Script", height=300, 
            help="Paste the full CREATE PROCEDURE script here.",
            key=f"txt_sp_{package_name}")
            
        if st.button("Analyze Stored Procedure", key=f"btn_sp_analyze_{package_name}"):
            if sp_content:
                # Use standard SQLParser
                parser = SQLParser()
                statements = sp_content.split(';')
                
                lineage_steps = []
                
                for idx, stmt in enumerate(statements):
                    stmt = stmt.strip()
                    if not stmt: continue
                    
                    # Extract metadata using new robust parser
                    meta = parser.extract_statement_metadata(stmt)
                    if meta and meta['Operation'] != 'UNKNOWN':
                        meta['Step ID'] = idx
                        meta['Statement Snippet'] = stmt[:100] + "..." if len(stmt)>100 else stmt
                        lineage_steps.append(meta)
                
                if lineage_steps:
                    st.success(f"Successfully extracted {len(lineage_steps)} operations!")
                    
                    # 1. Summary Table
                    summary_data = []
                    for step in lineage_steps:
                        summary_data.append({
                            'Step': step['Step ID'],
                            'Operation': step['Operation'],
                            'Destination': step['Destination'],
                            'Sources': ", ".join(step['Sources'])
                        })
                    
                    st.markdown("### 📋 Process Overview")
                    st.dataframe(pd.DataFrame(summary_data), use_container_width=True)
                    
                    # 2. Detailed Breakdown with Column Provenance
                    st.markdown("### 🕵️ Step-by-Step Provenance")
                    
                    for step in lineage_steps:
                        with st.expander(f"Step {step['Step ID']}: {step['Operation']} -> {step['Destination']}", expanded=True):
                            st.code(step['Statement Snippet'], language='sql')
                            
                            if step['Columns']:
                                st.write("**Column Lineage:**")
                                col_data = []
                                for tgt, info in step['Columns'].items():
                                    if isinstance(info, dict):
                                        col_data.append({
                                            'Target Column': tgt,
                                            'Source Column': info.get('source_column', 'N/A'),
                                            'Source Table': info.get('source_table', 'N/A'),
                                            'Expression': info.get('expression', '')
                                        })
                                    else:
                                        col_data.append({
                                            'Target Column': tgt,
                                            'Source Column': 'N/A',
                                            'Source Table': str(info),
                                            'Expression': ''
                                        })
                                
                                df_cols = pd.DataFrame(col_data)
                                st.dataframe(df_cols, use_container_width=True)
                            
                                if step.get('Join Keys'):
                                    st.caption("🧩 Join Logic & Keys")
                                    df_joins = pd.DataFrame(step['Join Keys'])
                                    # Map new keys to friendly names
                                    rename_map = {
                                        'left_table_alias': 'Alias 1',
                                        'left_table': 'Tabel Source 1',
                                        'left_column': 'Kolom 1',
                                        'right_table_alias': 'Alias 2',
                                        'right_table': 'Tabel Source 2',
                                        'right_column': 'Kolom 2',
                                        'join_type': 'Tipe Join'
                                    }
                                    df_display = df_joins.rename(columns=rename_map)
                                    # Show relevant columns
                                    display_cols = [v for k,v in rename_map.items() if v in df_display.columns]
                                    st.dataframe(df_display[display_cols], use_container_width=True)
                            else:
                                st.info("No explicit column mappings found (Wildcard or simple operation)")
                    
                    # 3. Viz
                    import graphviz
                    g = graphviz.Digraph()
                    g.attr(rankdir='LR')
                    
                    for step in lineage_steps:
                        dest_clean = re.sub(r'[^a-zA-Z0-9_]', '_', step['Destination'])
                        g.node(dest_clean, label=step['Destination'], shape='box', style='filled', color='lightblue')
                        
                        for src in step['Sources']:
                            src_clean = re.sub(r'[^a-zA-Z0-9_]', '_', src)
                            g.node(src_clean, label=src, shape='ellipse', color='lightgrey')
                            g.edge(src_clean, dest_clean, label=step['Operation'])
                    
                    st.graphviz_chart(g)
                else:
                    st.warning("No standard operations (INSERT/UPDATE/SELECT INTO) detected.")
            else:
                st.warning("Please paste the Stored Procedure script first.")

    # --- Sub-Tab 2: Views ---
    with sub_tab_view:
        st.markdown("Paste your `CREATE VIEW` script here to reverse-engineer its lineage.")
        
        view_content = st.text_area("View Script", height=300, 
            help="Paste the full CREATE VIEW script here.",
            key=f"txt_view_{package_name}")
            
        if st.button("Analyze View Lineage", key=f"btn_view_analyze_{package_name}"):
            if view_content:
                parser = SQLParser()
                # Treat view as single statement usually, but split by ; just in case
                statements = view_content.split(';')
                
                lineage_steps = []
                
                for idx, stmt in enumerate(statements):
                    stmt = stmt.strip()
                    if not stmt: continue
                    
                    meta = parser.extract_statement_metadata(stmt)
                    if meta and meta['Operation'] != 'UNKNOWN':
                        meta['Step ID'] = idx
                        lineage_steps.append(meta)
                
                if lineage_steps:
                    st.success(f"Successfully parse {len(lineage_steps)} View definition(s)!")
                    
                    for step in lineage_steps:
                        with st.expander(f"View: {step['Destination']}", expanded=True):
                            st.write(f"**Sources:** {', '.join(step['Sources'])}")
                            
                            if step['Columns']:
                                st.write("**Column Lineage:**")
                                col_data = []
                                for tgt, info in step['Columns'].items():
                                    if isinstance(info, dict):
                                        col_data.append({
                                            'View Column': tgt,
                                            'Source Column': info.get('source_column', 'N/A'),
                                            'Source Table': info.get('source_table', 'N/A'),
                                            'Expression': info.get('expression', '')
                                        })
                                st.dataframe(pd.DataFrame(col_data), use_container_width=True)
                            
                            # Join Logic for Views
                            if step.get('Join Keys'):
                                st.caption("🧩 Join Logic & Keys")
                                df_joins = pd.DataFrame(step['Join Keys'])
                                rename_map = {
                                    'left_table_alias': 'Alias 1',
                                    'left_table': 'Tabel Source 1',
                                    'left_column': 'Kolom 1',
                                    'right_table_alias': 'Alias 2',
                                    'right_table': 'Tabel Source 2',
                                    'right_column': 'Kolom 2',
                                    'join_type': 'Tipe Join'
                                }
                                df_display = df_joins.rename(columns=rename_map)
                                display_cols = [v for k,v in rename_map.items() if v in df_display.columns]
                                st.dataframe(df_display[display_cols], use_container_width=True)
                            
                            # Simple Graph
                            import graphviz
                            g = graphviz.Digraph()
                            g.attr(rankdir='LR')
                            dest_clean = re.sub(r'[^a-zA-Z0-9_]', '_', step['Destination'])
                            g.node(dest_clean, label=step['Destination'], shape='box', style='filled', color='lightblue')
                            for src in step['Sources']:
                                src_clean = re.sub(r'[^a-zA-Z0-9_]', '_', src)
                                g.node(src_clean, label=src, shape='ellipse', color='lightgrey')
                                g.edge(src_clean, dest_clean, label='SELECT')
                            st.graphviz_chart(g)
                else:
                    st.warning("No CREATE VIEW statement detected.")
            else:
                st.warning("Please paste the View definition first.")

def render_package_details(extractor, metadata, file_path=None):
    """Render details for a single package using the extractor instance and pre-computed metadata"""
    
    package_info = metadata['info']
    connections = metadata['connections']
    variables = metadata['variables']
    executables = metadata['executables']
    sources = metadata['sources']
    destinations = metadata['destinations']
    transformations = metadata['transformations']
    lineage = metadata['lineage']
    
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Package Name", package_info['Package Name'])
    with col2:
        st.metric("Creator", package_info['CreatorName'])
    with col3:
        st.metric("Created Date", package_info['CreationDate'])
        
    tab2, tab3, tab4, tab5, tab_qual, tab6, tab7, tab_refine, tab_sp, tab8 = st.tabs([
        " Data Sources", 
        "📤 Destinations", 
        ":arrows_counterclockwise: Transformations", 
        "🔗 Column Lineage",
        "🛡️ Quality & Validation",
        "Variables",
        "Tasks",
        "🛠️ SQL Refiner",
        "📜 SQL Script Analyzer",
        "💾 Export"
    ])
    with tab2:
        st.subheader("Data Sources")
        if sources:
            # Group sources by Data Flow Task
            from collections import defaultdict
            sources_by_flow = defaultdict(list)
            for source in sources:
                flow_name = source.get('Data Flow Task', 'Unknown')
                sources_by_flow[flow_name].append(source)
            
            # Display each data flow separately
            for flow_name, flow_sources in sources_by_flow.items():
                st.markdown(f"### 🔄 Data Flow: `{flow_name}`")
                st.caption(f"{len(flow_sources)} source component(s)")
                
                for idx, source in enumerate(flow_sources, 1):
                    with st.expander(f"📥 {source['Component Name']}", expanded=(idx==1 and len(sources_by_flow)==1)):
                        col1, col2 = st.columns(2)
                        with col1:
                            st.write(f"**Connection:** {source['Connection']}")
                            st.write(f"**Table/View:** {source['Table/View']}")
                            st.write(f"**Type:** {source['Component Type']}")
                        with col2:
                            st.write(f"**Column Count:** {len(source['Output Columns'])}")
                        
                        if source['SQL Query'] != 'N/A':
                            st.write("**SQL Query:**")
                            st.code(source['SQL Query'], language='sql')
                            
                            # NEW: Extract Join Keys for this source
                            try:
                                join_keys = extractor.parser.extract_join_keys(source['SQL Query'])
                                if join_keys:
                                    st.caption("🧩 Join Logic & Keys (Auto-Detected)")
                                    df_joins = pd.DataFrame(join_keys)
                                    cols = ['Original Table Alias', 'Original Column', 'Source Table', 'Source Column']
                                    final_cols = [c for c in cols if c in df_joins.columns]
                                    st.dataframe(df_joins[final_cols], use_container_width=True)
                            except Exception as e:
                                pass 
                        
                        if source['Output Columns']:
                            st.write("**Output Columns:**")
                            df_cols = pd.DataFrame(source['Output Columns'])
                            st.dataframe(df_cols, use_container_width=True)
                
                st.divider()
        else:
            # Check for Control Flow SQL Tasks
            sql_tasks = [
                e for e in executables 
                if 'ExecuteSQL' in e['Type'] or 'Execute SQL' in e['Type']
            ]
            
            if sql_tasks:
                 st.info("ℹ️ No Data Flow Pipeline found. This seems to be a **Control Flow (Stored Procedure)** package.")
                 st.markdown("### 🛠️ Procedure Calls / SQL Tasks")
                 
                 for task in sql_tasks:
                     with st.expander(f"⚡ {task['Task Name']} ({task['Type']})", expanded=True):
                         st.code(task['SQL Statement'], language='sql')
            else:
                st.info("No data sources found")
    
    with tab3:
        st.subheader("Destinations")
        if destinations:
            # Group destinations by Data Flow Task
            from collections import defaultdict
            dests_by_flow = defaultdict(list)
            for dest in destinations:
                flow_name = dest.get('Data Flow Task', 'Unknown')
                dests_by_flow[flow_name].append(dest)
            
            # Create a lookup for source tables using lineage
            dest_col_source_map = {}
            for item in lineage:
                key = (item['Destination Component'], item['Destination Column'])
                dest_col_source_map[key] = {
                    'Source Table': item['Source Table'],
                    'Original Column': item.get('Original Column', 'N/A'),
                    'Expression/Logic': item.get('Expression/Logic', '')
                }
                
            # Create a lookup for SQL Query by Data Flow Task (from Sources)
            dft_sql_map = {}
            if sources:
                for src in sources:
                    dft = src.get('Data Flow Task')
                    sql = src.get('SQL Query')
                    if dft and sql and sql != 'N/A':
                        dft_sql_map[dft] = sql
            
            # Display each data flow separately
            for flow_name, flow_dests in dests_by_flow.items():
                st.markdown(f"### 🔄 Data Flow: `{flow_name}`")
                st.caption(f"{len(flow_dests)} destination component(s)")
                
                for idx, dest in enumerate(flow_dests, 1):
                    with st.expander(f"📤 {dest['Component Name']}", expanded=(idx==1 and len(dests_by_flow)==1)):
                        col1, col2 = st.columns(2)
                        with col1:
                            st.write(f"**Connection:** {dest['Connection']}")
                            st.write(f"**Target Table:** {dest['Target Table']}")
                        with col2:
                            st.write(f"**Type:** {dest['Component Type']}")
                            st.write(f"**Column Count:** {len(dest['Input Columns'])}")
                        
                        if dest.get('SQL Query') and dest['SQL Query'] != 'N/A':
                            st.write("**SQL Query:**")
                            st.code(dest['SQL Query'], language='sql')
                        
                        # Extract and Display JOIN Keys (from Source, linked by Flow Name)
                        source_sql = dft_sql_map.get(flow_name)
                        if source_sql:
                             # Only show if not already shown via dest['SQL Query'] checking (deduplication)
                             # or if dest['SQL Query'] was missing. 
                             # Simpler: Just show Join Keys if we have source SQL.
                             try:
                                 join_keys = extractor.parser.extract_join_keys(source_sql)
                                 if join_keys:
                                     with st.expander("🧩 Join Logic & Keys", expanded=False):
                                         df_joins = pd.DataFrame(join_keys)
                                         cols = ['Original Table Alias', 'Original Column', 'Source Table', 'Source Column']
                                         final_cols = [c for c in cols if c in df_joins.columns]
                                         st.dataframe(df_joins[final_cols], use_container_width=True)
                             except Exception as e:
                                 pass # Silent fail if extraction issues
                                 
                        if dest['Input Columns']:
                            st.write("**Column Mappings:**")
                            
                            # Enrich input columns with source table info
                            enriched_cols = []
                            for col in dest['Input Columns']:
                                col_copy = col.copy()
                                key = (dest['Component Name'], col['Target Column'])
                                source_info = dest_col_source_map.get(key, {})
                                
                                col_copy['Source Table'] = source_info.get('Source Table', 'N/A')
                                col_copy['Original Column'] = source_info.get('Original Column', 'N/A')
                                col_copy['Expression/Logic'] = source_info.get('Expression/Logic', '')
                                
                                enriched_cols.append(col_copy)
                            
                            df_cols = pd.DataFrame(enriched_cols)
                            # Reorder columns to show Source Table prominently
                            cols_order = ['Source Column', 'Original Column', 'Source Table', 'Expression/Logic', 'Target Column', 'Data Type', 'Destination']
                            # Keep only columns that exist (in case structure changes)
                            final_cols = [c for c in cols_order if c in df_cols.columns]
                            st.dataframe(df_cols[final_cols], use_container_width=True)
                
                st.divider()
        else:
            st.info("No destinations found")
    
    with tab4:
        st.subheader("Transformations")
        if transformations:
            df_trans = pd.DataFrame(transformations)
            st.dataframe(df_trans, use_container_width=True, height=400)
            
            st.download_button(
                "📥 Download Transformations CSV",
                df_trans.to_csv(index=False),
                file_name="ssis_transformations.csv",
                mime="text/csv"
            )
        else:
            st.info("No transformations found")
    
    with tab5:
        st.subheader("🔗 Column Lineage (Source → Destination)")
        if lineage:
            df_lineage = pd.DataFrame(lineage)
            
            # --- Filtering ---
            with st.expander("🔎 Advanced Search & Filter", expanded=True):
                c1, c2 = st.columns(2)
                with c1:
                    filter_source = st.multiselect("Filter Source Table", options=sorted(df_lineage['Source Table'].astype(str).unique()))
                with c2:
                    filter_dest = st.multiselect("Filter Dest Table", options=sorted(df_lineage['Destination Table'].astype(str).unique()))
                
                search_term = st.text_input("Search Column Name (Source or Destination)", "")
            
            if filter_source:
                df_lineage = df_lineage[df_lineage['Source Table'].isin(filter_source)]
            if filter_dest:
                df_lineage = df_lineage[df_lineage['Destination Table'].isin(filter_dest)]
            if search_term:
                # Search in Source Column AND Destination Column
                mask = df_lineage['Source Column'].astype(str).str.contains(search_term, case=False, na=False) | \
                       df_lineage['Destination Column'].astype(str).str.contains(search_term, case=False, na=False)
                df_lineage = df_lineage[mask]
            
            st.dataframe(df_lineage, use_container_width=True, height=500)
            
            st.download_button(
                "📥 Download Column Lineage CSV",
                df_lineage.to_csv(index=False),
                file_name="ssis_column_lineage.csv",
                mime="text/csv"
            )
            
            # Create lookup for Source SQL
            source_sql_map = {}
            if sources:
                for src in sources:
                    comp_name = src.get('Component Name')
                    sql = src.get('SQL Query')
                    if comp_name and sql and sql != 'N/A':
                        source_sql_map[comp_name] = sql

            # Group by destination table
            st.subheader("📊 Lineage by Destination Table")
            for dest_table in df_lineage['Destination Table'].astype(str).unique():
                with st.expander(f"🎯 {dest_table}"):
                    df_table = df_lineage[df_lineage['Destination Table'] == dest_table]
                    st.dataframe(df_table, use_container_width=True)
                    
                    # Show Join Keys if available in source SQL
                    distinct_sources = df_table['Source Component'].dropna().unique()
                    for src_comp in distinct_sources:
                        sql = source_sql_map.get(src_comp)
                        if sql:
                            try:
                                join_keys = extractor.parser.extract_join_keys(sql)
                                if join_keys:
                                    st.caption(f"🧩 **Join Logic & Keys (Source: `{src_comp}`)**")
                                    df_joins = pd.DataFrame(join_keys)
                                    cols = ['Original Table Alias', 'Original Column', 'Source Table', 'Source Column']
                                    final_cols = [c for c in cols if c in df_joins.columns]
                                    st.dataframe(df_joins[final_cols], use_container_width=True)
                                else: # DEBUG
                                    st.info(f"KEYS EMPTY for {src_comp}")
                            except Exception as e:
                                st.error(f"Join extraction error: {e}")
                        else: # DEBUG 
                             st.info(f"NO SQL FOUND for {src_comp}")
        else:
            # Check for Control Flow SQL Tasks
            sql_tasks = [
                e for e in executables 
                if 'ExecuteSQL' in e['Type'] or 'Execute SQL' in e['Type']
            ]
            
            if sql_tasks:
                 st.info("ℹ️ Showing Control Flow Lineage (Stored Procedures)")
                 import graphviz
                 g = graphviz.Digraph()
                 g.attr(rankdir='LR')
                 
                 g.node('Start', shape='circle', style='filled', color='lightgrey')
                 g.node('End', shape='doublecircle', style='filled', color='lightgrey')
                 
                 for task in sql_tasks:
                     t_name = task['Task Name']
                     # Clean name for dot
                     safe_name = re.sub(r'[^a-zA-Z0-9_]', '_', t_name)
                     
                     g.node(safe_name, label=t_name, shape='box', style='filled', color='lightblue')
                     g.edge('Start', safe_name)
                     g.edge(safe_name, 'End')
                     
                 st.graphviz_chart(g)
            else:
                st.info("No column lineage found")
    
    with tab_qual:
        st.subheader("🛡️ Quality & Validation")
        
        # 1. Unused Columns Check
        try:
            # unused_report = extractor.get_unused_columns() 
            # Use metadata
            unused_report = metadata['unused']
            
            if unused_report:
                st.warning(f"Found {len(unused_report)} source components with unused columns!")
                df_unused = pd.DataFrame(unused_report)
                st.dataframe(df_unused, use_container_width=True)
                
                st.markdown("""
                > **Optimization Tip:** These columns are fetched from the database but never reach a destination. 
                > Removing them from the Source Query can improve SSIS performance and reduce network load.
                """)
            else:
                st.success("✅ Clean! All source columns are used in destinations.")
        except Exception as e:
            st.error(f"Quality Check Failed: {str(e)}")
            
        st.divider()
        # Keep existing dashboard if needed, or replace entirely.
        render_quality_dashboard(lineage) # Restored for Lineage Coverage %

    with tab6:
        st.subheader("Variables")
        if variables:
            df_vars = pd.DataFrame(variables)
            st.dataframe(df_vars, use_container_width=True, height=400)
            
            st.download_button(
                "📥 Download Variables CSV",
                df_vars.to_csv(index=False),
                file_name="ssis_variables.csv",
                mime="text/csv"
            )
        else:
            st.info("No variables found")
    
    with tab7:
        st.subheader("Tasks/Executables")
        if executables:
            df_exe = pd.DataFrame(executables)
            st.dataframe(df_exe, use_container_width=True, height=400)
            
            st.download_button(
                "📥 Download Tasks CSV",
                df_exe.to_csv(index=False),
                file_name="ssis_tasks.csv",
                mime="text/csv"
            )
        else:
            st.info("No tasks found")
    
    with tab_refine:
        st.subheader("🛠️ SQL Refiner & Standardization")
        st.info("Scan package for messy SQL (inconsistent keywords, aliases, etc.) and standardize it.")
        
        # Check session state for changes specific to this package
        changes_key = f'refine_changes_{package_info["Package Name"]}'
        
        c1, c2 = st.columns([1, 2])
        with c1:
            if st.button("Scan & Refine SQL Scripts", key=f"btn_refine_{package_info['Package Name']}"):
                changes = extractor.refine_package_sql()
                st.session_state[changes_key] = changes
                if not changes:
                    st.success("✅ All SQL scripts look standard!")
        
        if changes_key in st.session_state:
            changes = st.session_state[changes_key]
            
            if changes:
                st.warning(f"Found {len(changes)} components with potential improvements.")
                
                for idx, change in enumerate(changes):
                    with st.expander(f"📝 {change['component']}"):
                        col_a, col_b = st.columns(2)
                        with col_a:
                            st.caption("Original")
                            st.code(change['old'], language='sql')
                        with col_b:
                            st.caption("Refined")
                            st.code(change['new'], language='sql')
                
                st.divider()
                
                # Save Action
                if file_path:
                    if st.button("💾 Save Refined Package", key=f"btn_save_{package_info['Package Name']}"):
                        try:
                            # Re-register namespace to avoid ns0 prefixes
                            ET.register_namespace('', "www.microsoft.com/SqlServer/Dts")
                            ET.register_namespace('DTS', "www.microsoft.com/SqlServer/Dts")
                            
                            tree = ET.ElementTree(extractor.root)
                            tree.write(file_path, encoding='utf-8', xml_declaration=True)
                            
                            st.success(f"Successfully saved refined package to {file_path}")
                            st.balloons()
                            
                            # Clear state
                            del st.session_state[changes_key]
                            
                            # Optional: Trigger reload?
                            # st.experimental_rerun()
                            
                        except Exception as e:
                            st.error(f"Failed to save: {e}")
                else:
                    st.warning("Cannot save directly (File uploaded). Download the refined version below.")
                    rough_string = ET.tostring(extractor.root, encoding='utf-8')
                    st.download_button(
                        "📥 Download Refined .dtsx", 
                        rough_string, 
                        file_name=f"refined_{package_info['Package Name']}.dtsx",
                        mime="text/xml"
                    )

    with tab_sp:
        render_sql_script_analyzer(package_name=package_info['Package Name'])

    with tab8:
        st.subheader("💾 Export Complete Metadata")
        
        # Create comprehensive report
        report = f"""
# SSIS Package Metadata Report
Package: {package_info['Package Name']}
Date Extracted: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}

## Summary
- Connections: {len(connections)}
- Data Sources: {len(sources)}
- Destinations: {len(destinations)}
- Transformations: {len(transformations)}
- Variables: {len(variables)}
- Tasks: {len(executables)}
- Column Mappings: {len(lineage)}

## Package Details
{pd.DataFrame([package_info]).to_markdown()}

## Connections
{pd.DataFrame(connections).to_markdown() if connections else 'None'}

## Data Sources
"""
        
        for source in sources:
            report += f"\n### {source['Component Name']}\n"
            report += f"- Connection: {source['Connection']}\n"
            report += f"- Table/View: {source['Table/View']}\n"
            if source['SQL Query'] != 'N/A':
                report += f"- SQL: ```sql\n{source['SQL Query']}\n```\n"
            if source['Output Columns']:
                report += f"\nColumns:\n{pd.DataFrame(source['Output Columns']).to_markdown()}\n"
        
        report += "\n## Destinations\n"
        for dest in destinations:
            report += f"\n### {dest['Component Name']}\n"
            report += f"- Connection: {dest['Connection']}\n"
            report += f"- Target Table: {dest['Target Table']}\n"
            if dest['Input Columns']:
                report += f"\nColumns:\n{pd.DataFrame(dest['Input Columns']).to_markdown()}\n"
        
        if lineage:
            report += "\n## Column Lineage\n"
            report += pd.DataFrame(lineage).to_markdown()
        
        st.download_button(
            "📥 Download Complete Report (Markdown)",
            report,
            file_name=f"ssis_metadata_{package_info['Package Name']}.md",
            mime="text/markdown"
        )

# ==========================================
# Main Application Logic
# ==========================================

# Sidebar
st.sidebar.header("Input Settings")
source_mode = st.sidebar.radio("Select Input Mode", ["Upload Files", "Scan Local Folder", "Standalone SQL Analyzer"])

packages_to_process = [] # List of (filename, content) keys

if source_mode == "Standalone SQL Analyzer":
    st.info("Directly analyze Stored Procedures and View definitions.")
    render_sql_script_analyzer(package_name="Standalone")

elif source_mode == "Upload Files":
    uploaded_files = st.sidebar.file_uploader("Upload SSIS Packages (.dtsx)", type=['dtsx', 'xml'], accept_multiple_files=True)
    if uploaded_files:
        for f in uploaded_files:
            packages_to_process.append((f.name, f.read().decode('utf-8'), None))

elif source_mode == "Scan Local Folder":
    st.sidebar.info("Enter absolute path to folder containing .dtsx files")
    folder_path = st.sidebar.text_input("Folder Path")
    
    if folder_path:
        if os.path.exists(folder_path) and os.path.isdir(folder_path):
            files = sorted([f for f in os.listdir(folder_path) if f.lower().endswith(('.dtsx', '.xml'))])
            st.sidebar.success(f"Found {len(files)} package files.")
            
            # User Selection
            selected_files = st.sidebar.multiselect(
                "Select Packages to Process",
                options=files,
                default=None, # Default to None (empty) as requested
                help="Select specific packages or remove ones you don't want to process."
            )
            
            # Persist load state
            if st.sidebar.button("Process Selected Packages"):
                st.session_state['loaded_folder'] = folder_path
                st.session_state['selected_files'] = selected_files
            
            # Clear state if folder changes (optional validation)
            if st.session_state.get('loaded_folder') != folder_path:
                if 'selected_files' in st.session_state:
                    del st.session_state['selected_files']

            # Load logic
            if st.session_state.get('loaded_folder') == folder_path and 'selected_files' in st.session_state:
                target_files = st.session_state['selected_files']
                
                if not target_files:
                    st.warning("No packages selected!")
                else:
                    for f in target_files:
                        full_path = os.path.join(folder_path, f)
                        try:
                            with open(full_path, 'r', encoding='utf-8') as file:
                                packages_to_process.append((f, file.read(), full_path))
                        except Exception as e:
                            st.sidebar.error(f"Error reading {f}: {e}")
        else:
            st.sidebar.warning("Folder path does not exist or is not a directory.")

# Processing and Display
if packages_to_process:
    st.success(f"✅ Loaded {len(packages_to_process)} packages. Processing...")
    
    processed_packages = []
    
    try:
        with st.spinner("Extracting metadata..."):
            for fname, content, full_path in packages_to_process:
                # Use Cached Processing
                metadata = process_package_metadata(content)
                
                # Re-create lightweight extractor for Refinement usage (pass-through)
                # Or just use a fresh one (Parsing XML is fast)
                extractor = SSISMetadataExtractor(content) 

                processed_packages.append({
                    'filename': fname,
                    'extractor': extractor,
                    'metadata': metadata, # metadata dict
                    'info': metadata['info'],
                    'full_path': full_path
                })
        
        # Display Overview
        st.divider()
        st.header("📊 Global Overview")
        m1, m2 = st.columns(2)
        m1.metric("Total Packages Loaded", len(processed_packages))
        m2.metric("Total Processed", len(processed_packages))
        
        # Package Selector
        st.subheader("🔍 Inspect Package")
        
        pkg_options = {f"{p['info']['Package Name']} ({p['filename']})": i for i, p in enumerate(processed_packages)}
        selected_pkg_name = st.selectbox("Select Package", list(pkg_options.keys()))
        
        if selected_pkg_name:
            idx = pkg_options[selected_pkg_name]
            selected_pkg = processed_packages[idx]
            
            st.divider()
            st.markdown(f"### Currently Viewing: **{selected_pkg['info']['Package Name']}**")
            render_package_details(selected_pkg['extractor'], selected_pkg['metadata'], selected_pkg['full_path'])
            
    except Exception as e:
        st.error(f"❌ Error during processing: {str(e)}")
        st.exception(e)
else:
    st.info("👈 Select an input mode in the sidebar to get started.")
    
    st.markdown("""
    ## 🎯 Features for Migration
    
    ### What This Tool Extracts:
    
    1. **📦 Package Information**
       - Package name, creator, creation date
    
    2. **🔌 Connection Managers**
       - Connection names, types, connection strings
    
    3. **📥 Data Sources & 📤 Destinations**
       - Tables, Views, SQL Queries
       - **Auto-detected Source Tables** (even for derived columns)
    
    4. **🔗 Complete Column Lineage**
       - End-to-end tracking from Source to Destination
       - Handling of Transformations (Derived Column, Merge Join, etc.)
    
    5. **💾 Flexible Input**
       - Upload multiple .dtsx files
       - Scan local folders for bulk processing
    """)