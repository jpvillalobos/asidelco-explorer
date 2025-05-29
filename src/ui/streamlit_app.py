# streamlit_app.py
import streamlit as st

st.title("Buscador CFIA Inteligente")

question = st.text_input("Haz tu pregunta:")
if question:
    response = final_chain.invoke(question)
    st.write(response)