from htbuilder.units import rem
from htbuilder import div, styles
from collections import namedtuple
from concurrent.futures import ThreadPoolExecutor
import datetime
from pathlib import Path
import sys
import textwrap
import time

import pandas as pd
import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
	sys.path.insert(0, str(PROJECT_ROOT))

from app.config import settings
from app.database import engine
from app.db.schema import ensure_schema
from app.ingestion.mapping.embedding_mapper import EmbeddingBackend
from app.nl2sql.query_service import QueryService
from app.nl2sql.sql_generator import build_llm_backend


st.set_page_config(page_title="Streamlit AI assistant", page_icon="тЬи", initial_sidebar_state="collapsed")

st.markdown("""
<style>
/* Make chat input fully rounded */
[data-testid="stChatInput"] {
    border-radius: 30px !important;
}
/* For the inner container */
[data-testid="stChatInput"] > div {
    border-radius: 30px !important;
}
/* Ensure the text area also respects the rounding */
[data-testid="stChatInputTextArea"] {
    border-radius: 30px !important;
}

/* Make send button circular/rounded */
[data-testid="stChatInputSubmitButton"] {
    border-radius: 50% !important;
}

/* Hide some default Streamlit elements for a cleaner look */
#MainMenu {visibility: hidden;}
footer {visibility: hidden;}
</style>
""", unsafe_allow_html=True)

# -----------------------------------------------------------------------------
# Sidebar UI
with st.sidebar:
    st.button("New Chat", use_container_width=True)
    st.divider()
    
    st.markdown("**History**")
    st.button("Q1 Queries", use_container_width=True)
    st.button("Loan analysis", use_container_width=True)
    st.button("2024 Payments", use_container_width=True)
        
    st.markdown("""
        <style>
        /* Reduce the huge default top padding of the sidebar to pull 'New Chat' up */
        [data-testid="stSidebarUserContent"] {
            padding-top: 1rem !important; 
        }

        /* Pull the divider (hr) upwards to sit closer to the New Chat button */
        [data-testid="stSidebarUserContent"] hr {
            margin-top: 0.5rem !important;
            margin-bottom: 0.5rem !important;
        }

        /* Add shadow to the sidebar to detach it from the background */
        [data-testid="stSidebar"] {
            box-shadow: 4px 0 15px rgba(0, 0, 0, 0.08) !important;
        }
        
        /* Left align all sidebar buttons and give them a subtle border */
        [data-testid="stSidebar"] div.stButton > button {
            justify-content: flex-start !important;
            text-align: left !important;
            border: 1px solid rgba(0, 0, 0, 0.1) !important;
            border-radius: 8px !important;
            box-shadow: none;
            background-color: white;
            padding: 0.25rem 0.75rem !important;
            min-height: 2.2rem !important;
        }
        
        /* Reduce the gap Streamlit puts between elements internally */
        [data-testid="stSidebarUserContent"] > div.element-container {
            margin-bottom: -0.5rem;
        }

        /* Slight color change on hover to make it obvious they are clickable buttons */
        [data-testid="stSidebar"] div.stButton > button:hover {
            border-color: rgba(0, 0, 0, 0.2) !important;
            background-color: #f9f9f9;
        }

        /* Float the last element (Settings button) */
        [data-testid="stSidebarUserContent"] > div.element-container:last-child {
            position: fixed !important;
            bottom: 30px !important;
            left: 20px !important;
            width: calc(336px - 40px) !important; /* Default sidebar width 336px minus margins */
            z-index: 999 !important;
            background-color: #fafafa !important;
            padding-top: 10px;
        }
        
        /* Ensure Settings button hover looks normal since background is inherited */
        [data-testid="stSidebarUserContent"] > div.element-container:last-child button:hover {
            background-color: #f0f2f6;
        }
        </style>
    """, unsafe_allow_html=True)
    
    if st.button("Settings", use_container_width=True):
        st.session_state._show_settings = True


@st.cache_resource(ttl="1h")
def get_query_service():
	ensure_schema(engine)

	embedder = EmbeddingBackend(
		settings.embedding_model_path,
		provider=settings.embedding_provider,
		ollama_base_url=settings.embedding_base_url,
		ollama_timeout_s=settings.embedding_timeout_s,
	)
	llm_backend = build_llm_backend(
		model_name=settings.llm_model_name,
		base_url=settings.llm_base_url,
		timeout_s=settings.llm_timeout_s,
	)
	
	service = QueryService.build(
		engine,
		embedder,
		llm_backend,
		max_new_tokens=settings.llm_max_new_tokens,
	)
	return service


executor = ThreadPoolExecutor(max_workers=5)

MODEL = "configured-via-env"

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

	settings, backend = get_llm_runtime()
	return backend.generate(prompt, max_new_tokens=max(128, settings.llm_max_new_tokens // 4))


def history_to_text(chat_history):
	"""Converts chat history into a string."""
	return "\n".join(f"[{h['role']}]: {h['content']}" for h in chat_history)


def search_relevant_pages(query):
	"""Searches the markdown contents of Streamlit's documentation."""
	del query
	return "\n".join(
		[
			f"[Snowflake AI features]: {CORTEX_URL}",
			f"[Streamlit assistant example]: {GITHUB_URL}",
		]
	)


def search_relevant_docstrings(query):
	"""Searches the docstrings of Streamlit's commands."""
	del query
	return "\n".join(
		[
			"[Document 0]: st.chat_input(prompt) creates a chat input box.",
			"[Document 1]: st.chat_message(role) renders messages in a conversational layout.",
			"[Document 2]: st.pills(options=...) can render quick selectable suggestions.",
		]
	)


def _stream_text(text: str, chunk_size: int = 80):
	for i in range(0, len(text), chunk_size):
		yield text[i : i + chunk_size]


def get_response(prompt):
	settings, backend = get_llm_runtime()
	response = backend.generate(prompt, max_new_tokens=settings.llm_max_new_tokens)
	return _stream_text(response)


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
				history_id = st.session_state.messages[message_index].get("history_id")
				if history_id:
					try:
						service = get_query_service()
						from sqlalchemy import text
						with service.engine.begin() as conn:
							conn.execute(
								text(
									"""
									UPDATE afm.query_history
									SET user_feedback = :rating,
										rating = :rating,
										feedback_note = :details,
										feedback_at = now()
									WHERE id = CAST(:id AS uuid)
									"""
								),
								{
									"rating": rating + 1 if rating is not None else 0,
									"details": details,
									"id": history_id,
								}
							)
						st.success("Thank you for your feedback!")
					except Exception as e:
						st.error(f"Failed to submit feedback: {e}")
				else:
					st.warning("Could not submit feedback (missing query context).")
				del rating, details, relevant_history
				pass


@st.dialog("AI Assistant")
def show_about_dialog():
	st.markdown("""
		**Welcome to the Financial Data Query Platform**
		
		This platform empowers you to interactively explore and analyze processed financial records using natural language.

		**Key Capabilities:**
		- **Natural Language to SQL:** Ask questions in plain English, and the system instantly translates them into SQL to query your database.
		- **Seamless Bank Statement Analysis:** Access structured data ingested seamlessly from various financial sources (e.g., Kaspi, Halyk bank statements).
		- **Self-Healing Queries:** The platform features an intelligent retry and repair logic that automatically fixes SQL syntax or schema mismatch errors.
		- **Automated Insights:** Turns raw database rows into easy-to-read, conversational summaries based on the exact results of the data.
		""")


@st.dialog("Settings")
def show_settings_dialog():
	st.markdown("### NL2SQL Pipeline Parameters")
	st.session_state.max_rows = st.slider(
		"Max Rows",
		min_value=10,
		max_value=1000,
		value=st.session_state.get("max_rows", 100),
		step=10,
		help="Maximum number of rows fetched/returned from the database."
	)
	
	st.session_state.retry_attempts = st.slider(
		"Retry Attempts (Retry Budget)",
		min_value=0,
		max_value=5,
		value=st.session_state.get("retry_attempts", 2),
		help="How many times the NL2SQL pipeline should attempt to automatically repair failing SQL queries."
	)
	
	if st.button("Save Settings", use_container_width=True):
		st.session_state._show_settings = False
		st.rerun()

if st.session_state.get("_show_settings", False):
	show_settings_dialog()

st.html(div(style=styles(font_size=rem(5), line_height=1))["тЭЙ"])

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
		del selected_suggestion

	st.button(
		"&nbsp;:small[:gray[:material/info: AI Assistant]]",
		type="tertiary",
		on_click=show_about_dialog,
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
		
		# Display the SQL block if present in the message
		if message.get("sql_block"):
			with st.expander(message.get("sql_header", "**Generated SQL**")):
				st.markdown(message["sql_block"])

		if message.get("rows") is not None:
			st.dataframe(message["rows"])

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

		# Send prompt to NL2SQL Pipeline.
		with st.spinner("Processing query via NL2SQL Pipeline..."):
			service = get_query_service()
			service.executor.max_rows = st.session_state.get("max_rows", 100)
			service.repair.max_attempts = st.session_state.get("retry_attempts", 2)
			
			try:
				result = service.run(user_message)
				
				if result.error:
					response_text = f"**Error:**\n```text\n{result.error}\n```"
					sql_block = None
				else:
					repaired_msg = " *(Auto-repaired)*" if result.repaired else ""
					sql_header = f"**Generated SQL ({result.execution_time_s:.2f}s){repaired_msg}**"
					sql_block = f"```sql\n{result.sql}\n```"
					
					base_text = f"\n\n**Results ({len(result.rows)} rows)**"
					
					if result.rows:
						with st.spinner("AI is summarizing the data..."):
							answer_prompt = f"User asked: {user_message}\nSQL generated: {result.sql}\nData sample:\n{result.rows[:10]}\n\nProvide a short, direct natural language answer to the user's question based strictly on the returned data. Do not explain the SQL."
							ai_answer = service.generator.backend.generate(answer_prompt, max_new_tokens=400)
						response_text = f"{ai_answer}{base_text}"
					else:
						response_text = f"No data found for your query.{base_text}"

			except Exception as e:
				response_text = f"**Pipeline Failed:**\n```text\n{e}\n```"
				sql_block = None
				sql_header = None
				result = None

		# Put everything after the spinners in a container to fix the
		# ghost message bug.
		with st.container():
			# Stream textual response.
			def _stream_result(text):
				for idx in range(0, len(text), 40):
					yield text[idx:idx+40]

			response = st.write_stream(_stream_result(response_text))
			
			if sql_block:
				with st.expander(sql_header):
					st.markdown(sql_block)
					
			rows_to_save = None
			if result and not result.error and result.rows:
				df = pd.DataFrame(result.rows)
				# Convert strictly object columns like UUID/datetime to string for PyArrow compatibility
				for col in df.columns:
					if df[col].dtype == 'object':
						df[col] = df[col].astype(str)
				
				st.dataframe(df)
				rows_to_save = df

			# Add messages to chat history.
			st.session_state.messages.append({"role": "user", "content": user_message})
			msg_dict = {
				"role": "assistant",
				"content": response,
				"rows": "true_flag" if rows_to_save is not None else None,
				"sql_header": sql_header if 'sql_header' in locals() else None,
				"sql_block": sql_block if 'sql_block' in locals() else None
			}
			if result and result.history_id:
				msg_dict["history_id"] = result.history_id
			st.session_state.messages.append(msg_dict)
			
			if rows_to_save is not None:
				# Cache the actual dataframe in chat history
				st.session_state.messages[-1]["rows"] = rows_to_save

			# Other stuff.
			show_feedback_controls(len(st.session_state.messages) - 1)
			send_telemetry(question=user_message, response=response)
