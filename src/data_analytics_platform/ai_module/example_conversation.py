from data_analytics_platform.database.connection import DatabaseConnection
from data_analytics_platform.database.config import DatabaseConfig
from data_analytics_platform.ai_module.conversation_handler import AIConversationHandler
from data_analytics_platform.ai_module import LLMAdapter  # You'll need to implement this
from data_analytics_platform.database import QueryExecutor


def conversation_example():
    """Example of using the AI conversation handler."""
    try:
        # Set up database connection
        connection_string = DatabaseConfig.get_connection_string(
            db_type=DatabaseConfig.SQLITE,
            database="example.db"
        )
        db_connection = DatabaseConnection(connection_string)
        db_connection.connect()

        # Create query executor
        query_executor = QueryExecutor(db_connection)

        # Create LLM adapter (you'll need to implement this)
        llm_adapter = LLMAdapter(api_key="your_api_key")

        # Create conversation handler
        conversation = AIConversationHandler(
            ai_query_generator=llm_adapter,
            query_executor=query_executor,
            db_connection=db_connection
        )

        # Example conversation
        response = conversation.process_user_query("What tables are in the database?")
        print("User: What tables are in the database?")
        print(f"AI: {response['response_text']}")

        if response.get("suggested_followups"):
            print("\nSuggested follow-ups:")
            for suggestion in response["suggested_followups"]:
                print(f"- {suggestion}")

        # Example data query
        response = conversation.process_user_query("Show me the first 5 rows of the users table")
        print("\nUser: Show me the first 5 rows of the users table")
        print(f"AI: {response['response_text']}")

        if response.get("sql_query"):
            print(f"\nSQL Query: {response['sql_query']}")

        # Close connection
        db_connection.disconnect()

    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    conversation_example()