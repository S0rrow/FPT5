from numpy import empty
import streamlit as st
import pandas as pd
def display_filter_log(logger):
    logger.log(f"rendering filter_log page...", name=__name__)
    try:
        st.subheader("Filter Log")
        clear_btn = st.button("Clear Filter Log")
        if clear_btn:
            st.session_state['filter_log'] = []
        ### filter_log를 보여주되, 값이 존재하는 column의 경우에만 value를 보여주는 방식으로 보여줌
        filter_log = st.session_state.get('filter_log', [])
        ### json으로 보여주는게 아니라 column에 대해서 value만 보여주는 방식
        if filter_log:
            # st.columns where col1 is index, col2 is contents of filters
            # combine contents of filters into single string
            # and st.write #{index}: {filter_contents}
            filter_data = []
            for idx, filter in enumerate(filter_log):
                filter_row = {"Index": idx + 1}
                filter_contents = ""
                for key, value in filter.items():
                    if value is not None and value != []:
                        filter_contents += f"{key}: {value}, "
                filter_contents = filter_contents.rstrip(", ")
                filter_row["Filter"] = filter_contents
                filter_data.append(filter_row)
            
            if filter_data:
                filter_df = pd.DataFrame(filter_data, index=None)
                st.dataframe(filter_df)
            else:
                st.write("No filter logs found")
        else:
            st.write("No filter logs found")

    except Exception as e:
        logger.log(f"Exception occurred while rendering filter_log: {e}", name=__name__, flag=1)
        st.write("Something went wrong while loading your information :(")
