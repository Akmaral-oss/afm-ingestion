
from htbuilder.units import rem
from htbuilder import div, styles
from collections import namedtuple
from concurrent.futures import ThreadPoolExecutor
import datetime
import textwrap
import time

import streamlit as st
from snowflake.core import Root
from snowflake.cortex import complete


st.set_page_config(page_title="Streamlit AI assistant", page_icon="✨")

# -----------------------------------------------------------------------------
# Set things up.


@st.cache_resource(ttl="5m")
def get_session():
    return st.connection("snowflake").session()


root = Root(get_session())
executor = ThreadPoolExecutor(max_workers=5)

MODEL = "claude-3-5-sonnet"

DB = "ST_ASSISTANT"
SCHEMA = "PUBLIC"
DOCSTRINGS_SEARCH_SERVICE = "STREAMLIT_DOCSTRINGS_SEARCH_SERVICE"
PAGES_SEARCH_SERVICE = "STREAMLIT_DOCS_PAGES_SEARCH_SERVICE"
HISTORY_LENGTH = 5
SUMMARIZE_OLD_HISTORY = True
DOCSTRINGS_CONTEXT_LEN = 10
PAGES_CONTEXT_LEN = 10
MIN_TIME_BETWEEN_REQUESTS = datetime.timedelta(seconds=3)

CORTEX_URL = (
    "https://docs.snowflake.com/en/guides-overview-ai-features"
    "?utm_source=streamlit"
    "&utm_medium=referral"
    "&utm_campaign=streamlit-demo-apps"
    "&utm_content=streamlit-assistant"
)

GITHUB_URL = "https://github.com/streamlit/streamlit-assistant"

DEBUG_MODE = st.query_params.get("debug", "false").lower() == "true"

INSTRUCTIONS = textwrap.dedent("""
    - You are a helpful AI chat assistant focused on answering quesions about
      Streamlit, Streamlit Community Cloud, Snowflake, and general Python.
    - You will be given extra information provided inside tags like this
      <foo></foo>.
    - Use context and history to provide a coherent answer.
    - Use markdown such as headers (starting with ##), code blocks, bullet
      points, indentation for sub bullets, and backticks for inline code.
    - Don't start the response with a markdown header.
    - Assume the user is a newbie.
    - Be brief, but clear. If needed, you can write paragraphs of text, like
      a documentation website.
    - Avoid experimental and private APIs.
    - Provide examples.
    - Include related links throughout the text and at the bottom.
    - Don't say things like "according to the provided context".
    - Streamlit is a product of Snowflake.
    - Offer alternatives within the Streamlit and Snowflake universe.
    - For information about deploying in Snowflake, see
      https://www.snowflake.com/en/product/features/streamlit-in-snowflake/
""")

SUGGESTIONS = {
    ":blue[:material/local_library:] What is Streamlit?": (
        "What is Streamlit, what is it great at, and what can I do with it?"
    ),
    ":green[:material/database:] Help me understand session state": (
        "Help me understand session state. What is it for? "
        "What are gotchas? What are alternatives?"
    ),
    ":orange[:material/multiline_chart:] How do I make an interactive chart?": (
        "How do I make a chart where, when I click, another chart updates? "
        "Show me examples with Altair or Plotly."
    ),
    ":violet[:material/apparel:] How do I customize my app?": (
        "How do I customize my app? What does Streamlit offer? No hacks please."
    ),
    ":red[:material/deployed_code:] Deploying an app at work": (
        "How do I deploy an app at work? Give me easy and performant options."
    ),
}


def build_prompt(**kwargs):
    """Builds a prompt string with the kwargs as HTML-like tags.

    For example, this:

        build_prompt(foo="1\n2\n3", bar="4\n5\n6")

    ...returns:

        '''
        <foo>
        1
        2
        3
        </foo>
        <bar>
        4
        5
        6
        </bar>
        '''
    """
    prompt = []

    for name, contents in kwargs.items():
        if contents:
            prompt.append(f"<{name}>\n{contents}\n</{name}>")

    prompt_str = "\n".join(prompt)

    return prompt_str


# Just some little objects to make tasks more readable.
TaskInfo = namedtuple("TaskInfo", ["name", "function", "args"])
TaskResult = namedtuple("TaskResult", ["name", "result"])


def build_question_prompt(question):
    """Fetches info from different services and creates the prompt string."""
    old_history = st.session_state.messages[:-HISTORY_LENGTH]
    recent_history = st.session_state.messages[-HISTORY_LENGTH:]

    if recent_history:
        recent_history_str = history_to_text(recent_history)
    else:
        recent_history_str = None

    # Fetch information from different services in parallel.
    task_infos = []

    if SUMMARIZE_OLD_HISTORY and old_history:
        task_infos.append(
            TaskInfo(
                name="old_message_summary",
                function=generate_chat_summary,
                args=(old_history,),
            )
        )

    if PAGES_CONTEXT_LEN:
        task_infos.append(
            TaskInfo(
                name="documentation_pages",
                function=search_relevant_pages,
                args=(question,),
            )
        )

    if DOCSTRINGS_CONTEXT_LEN:
        task_infos.append(
            TaskInfo(
                name="command_docstrings",
                function=search_relevant_docstrings,
                args=(question,),
            )
        )

    results = executor.map(
        lambda task_info: TaskResult(
            name=task_info.name,
            result=task_info.function(*task_info.args),
        ),
        task_infos,
    )

    context = {name: result for name, result in results}

    return build_prompt(
        instructions=INSTRUCTIONS,
        **context,
        recent_messages=recent_history_str,
        question=question,
    )


def generate_chat_summary(messages):
    """Summarizes the chat history in `messages`."""
    prompt = build_prompt(
        instructions="Summarize this conversation as concisely as possible.",
        conversation=history_to_text(messages),
    )

    return complete(MODEL, prompt, session=get_session())


def history_to_text(chat_history):
    """Converts chat history into a string."""
    return "\n".join(f"[{h['role']}]: {h['content']}" for h in chat_history)


def search_relevant_pages(query):
    """Searches the markdown contents of Streamlit's documentation."""
    cortex_search_service = (
        root.databases[DB].schemas[SCHEMA].cortex_search_services[PAGES_SEARCH_SERVICE]
    )

    context_documents = cortex_search_service.search(
        query,
        columns=["PAGE_URL", "PAGE_CHUNK"],
        filter={},
        limit=PAGES_CONTEXT_LEN,
    )

    results = context_documents.results

    context = [f"[{row['PAGE_URL']}]: {row['PAGE_CHUNK']}" for row in results]
    context_str = "\n".join(context)

    return context_str


def search_relevant_docstrings(query):
    """Searches the docstrings of Streamlit's commands."""
    cortex_search_service = (
        root.databases[DB]
        .schemas[SCHEMA]
        .cortex_search_services[DOCSTRINGS_SEARCH_SERVICE]
    )

    context_documents = cortex_search_service.search(
        query,
        columns=["STREAMLIT_VERSION", "COMMAND_NAME", "DOCSTRING_CHUNK"],
        filter={"@eq": {"STREAMLIT_VERSION": "latest"}},
        limit=DOCSTRINGS_CONTEXT_LEN,
    )

    results = context_documents.results

    context = [
        f"[Document {i}]: {row['DOCSTRING_CHUNK']}" for i, row in enumerate(results)
    ]
    context_str = "\n".join(context)

    return context_str


def get_response(prompt):
    return complete(
        MODEL,
        prompt,
        stream=True,
        session=get_session(),
    )


def send_telemetry(**kwargs):
    """Records some telemetry about questions being asked."""
    # TODO: Implement this.
    pass


def show_feedback_controls(message_index):
    """Shows the "How did I do?" control."""
    st.write("")

    with st.popover("How did I do?"):
        with st.form(key=f"feedback-{message_index}", border=False):
            with st.container(gap=None):
                st.markdown(":small[Rating]")
                rating = st.feedback(options="stars")

            details = st.text_area("More information (optional)")

            if st.checkbox("Include chat history with my feedback", True):
                relevant_history = st.session_state.messages[:message_index]
            else:
                relevant_history = []

            ""  # Add some space

            if st.form_submit_button("Send feedback"):
                # TODO: Submit feedback here!
                pass


@st.dialog("Legal disclaimer")
def show_disclaimer_dialog():
    st.caption("""
            This AI chatbot is powered by Snowflake and public Streamlit
            information. Answers may be inaccurate, inefficient, or biased.
            Any use or decisions based on such answers should include reasonable
            practices including human oversight to ensure they are safe,
            accurate, and suitable for your intended purpose. Streamlit is not
            liable for any actions, losses, or damages resulting from the use
            of the chatbot. Do not enter any private, sensitive, personal, or
            regulated data. By using this chatbot, you acknowledge and agree
            that input you provide and answers you receive (collectively,
            “Content”) may be used by Snowflake to provide, maintain, develop,
            and improve their respective offerings. For more
            information on how Snowflake may use your Content, see
            https://streamlit.io/terms-of-service.
        """)


# -----------------------------------------------------------------------------
# Draw the UI.


st.html(div(style=styles(font_size=rem(5), line_height=1))["❉"])

title_row = st.container(
    horizontal=True,
    vertical_alignment="bottom",
)

with title_row:
    st.title(
        # ":material/cognition_2: Streamlit AI assistant", anchor=False, width="stretch"
        "Streamlit AI assistant",
        anchor=False,
        width="stretch",
    )

user_just_asked_initial_question = (
    "initial_question" in st.session_state and st.session_state.initial_question
)

user_just_clicked_suggestion = (
    "selected_suggestion" in st.session_state and st.session_state.selected_suggestion
)

user_first_interaction = (
    user_just_asked_initial_question or user_just_clicked_suggestion
)

has_message_history = (
    "messages" in st.session_state and len(st.session_state.messages) > 0
)

# Show a different UI when the user hasn't asked a question yet.
if not user_first_interaction and not has_message_history:
    st.session_state.messages = []

    with st.container():
        st.chat_input("Ask a question...", key="initial_question")

        selected_suggestion = st.pills(
            label="Examples",
            label_visibility="collapsed",
            options=SUGGESTIONS.keys(),
            key="selected_suggestion",
        )

    st.button(
        "&nbsp;:small[:gray[:material/balance: Legal disclaimer]]",
        type="tertiary",
        on_click=show_disclaimer_dialog,
    )

    st.stop()

# Show chat input at the bottom when a question has been asked.
user_message = st.chat_input("Ask a follow-up...")

if not user_message:
    if user_just_asked_initial_question:
        user_message = st.session_state.initial_question
    if user_just_clicked_suggestion:
        user_message = SUGGESTIONS[st.session_state.selected_suggestion]

with title_row:

    def clear_conversation():
        st.session_state.messages = []
        st.session_state.initial_question = None
        st.session_state.selected_suggestion = None

    st.button(
        "Restart",
        icon=":material/refresh:",
        on_click=clear_conversation,
    )

if "prev_question_timestamp" not in st.session_state:
    st.session_state.prev_question_timestamp = datetime.datetime.fromtimestamp(0)

# Display chat messages from history as speech bubbles.
for i, message in enumerate(st.session_state.messages):
    with st.chat_message(message["role"]):
        if message["role"] == "assistant":
            st.container()  # Fix ghost message bug.

        st.markdown(message["content"])

        if message["role"] == "assistant":
            show_feedback_controls(i)

if user_message:
    # When the user posts a message...

    # Streamlit's Markdown engine interprets "$" as LaTeX code (used to
    # display math). The line below fixes it.
    user_message = user_message.replace("$", r"\$")

    # Display message as a speech bubble.
    with st.chat_message("user"):
        st.text(user_message)

    # Display assistant response as a speech bubble.
    with st.chat_message("assistant"):
        with st.spinner("Waiting..."):
            # Rate-limit the input if needed.
            question_timestamp = datetime.datetime.now()
            time_diff = question_timestamp - st.session_state.prev_question_timestamp
            st.session_state.prev_question_timestamp = question_timestamp

            if time_diff < MIN_TIME_BETWEEN_REQUESTS:
                time.sleep(time_diff.seconds + time_diff.microseconds * 0.001)

            user_message = user_message.replace("'", "")

        # Build a detailed prompt.
        if DEBUG_MODE:
            with st.status("Computing prompt...") as status:
                full_prompt = build_question_prompt(user_message)
                st.code(full_prompt)
                status.update(label="Prompt computed")
        else:
            with st.spinner("Researching..."):
                full_prompt = build_question_prompt(user_message)

        # Send prompt to LLM.
        with st.spinner("Thinking..."):
            response_gen = get_response(full_prompt)

        # Put everything after the spinners in a container to fix the
        # ghost message bug.
        with st.container():
            # Stream the LLM response.
            response = st.write_stream(response_gen)

            # Add messages to chat history.
            st.session_state.messages.append({"role": "user", "content": user_message})
            st.session_state.messages.append({"role": "assistant", "content": response})

            # Other stuff.
            show_feedback_controls(len(st.session_state.messages) - 1)
            send_telemetry(question=user_message, response=response)