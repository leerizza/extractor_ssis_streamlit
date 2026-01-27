import streamlit as st
import pandas as pd

def render_quality_dashboard(lineage):
    """Render Data Quality & Maturity Stats"""
    st.markdown("### 🛡️ Data Quality Dashboard")
    
    if not lineage:
        st.info("No lineage data available for analysis.")
        return

    # 1. Calculation
    total_cols = len(lineage)
    
    # Define what counts as "Mapped"
    # Expression/Literal is considered mapped (it has known origin logic)
    # N/A or Unknown or empty is Unmapped
    
    unmapped_criteria = ['N/A', 'Unknown', '', 'None']
    
    mapped_cols = [l for l in lineage if l.get('Source Table', 'N/A') not in unmapped_criteria]
    mapped_count = len(mapped_cols)
    
    score = (mapped_count / total_cols) * 100 if total_cols > 0 else 0
    
    # 2. Metrics Row
    c1, c2, c3 = st.columns(3)
    with c1:
        st.metric("Traceability Score", f"{score:.1f}%", help="Percentage of destination columns traced back to a source table or valid expression.")
    with c2:
        st.metric("Total Mapped", f"{mapped_count} / {total_cols}")
    with c3:
        st.metric("Orphaned Columns", total_cols - mapped_count, delta_color="inverse")
        
    st.divider()
    
    # 3. Component Stats
    st.subheader("📊 Source Usage Stats")
    
    # Count mappings by Source Table
    source_counts = {}
    for l in lineage:
        src = l.get('Source Table', 'N/A')
        # Split by comma for multi-source
        if ',' in src:
            parts = [p.strip() for p in src.split(',')]
            for p in parts:
                source_counts[p] = source_counts.get(p, 0) + 1
        else:
            source_counts[src] = source_counts.get(src, 0) + 1
            
    # Convert to DF
    df_stats = pd.DataFrame(list(source_counts.items()), columns=['Source Table', 'Column Count'])
    df_stats = df_stats.sort_values(by='Column Count', ascending=False).head(10)
    
    st.bar_chart(df_stats.set_index('Source Table'))
    
    # 4. Orphan Analysis
    st.subheader("🔍 Orphaned Columns (Unmapped)")
    orphans = [l for l in lineage if l.get('Source Table', 'N/A') in unmapped_criteria]
    
    if orphans:
        st.warning(f"Found {len(orphans)} columns with missing lineage. These may be hardcoded or dynamic.")
        df_orphans = pd.DataFrame(orphans)
        # Simplify display
        display_cols = ['Destination Table', 'Destination Column', 'Source Component']
        # Handle cases where keys might be missing
        safe_cols = [c for c in display_cols if c in df_orphans.columns]
        st.dataframe(df_orphans[safe_cols], use_container_width=True)
    else:
        st.success("🎉 All columns are successfully mapped!")
