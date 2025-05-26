#!/usr/bin/env python3
import os
import signal
import logging
import constants
from flask import Flask, request, jsonify, Response
from flask_cors import CORS
from initializer import insights_generator
from metadata_generation import metadata_generator
from ask_summary_generation import ask_summary_generator

app_logger = logging.getLogger(__name__)
app = Flask(__name__)
CORS(app)


def clear_application_log_file():
    if os.path.exists(constants.LOG_FILE_PATH):
        try:
            # Open the file in 'write' mode ('w') which truncates it (clears content)
            with open(constants.LOG_FILE_PATH, 'w') as f:
                f.truncate(0) 
            app_logger.info(f"Log file '{constants.LOG_FILE_PATH}' successfully cleared.")
        except OSError as e:
            app_logger.error(f"Error clearing log file '{constants.LOG_FILE_PATH}': {e}", exc_info=True)
    else:
        app_logger.info(f"Log file '{constants.LOG_FILE_PATH}' does not exist, nothing to clear.")


def stop_gunicorn():
    """
    Stops the Gunicorn master process.  This should be called after 
    application has finished processing its task and sent the response.
    """
    server_pid = os.getppid()  # Get the parent process ID (Gunicorn master)
    os.kill(server_pid, signal.SIGTERM)  # Send a termination signal


@app.route("/metadata", methods=["POST"])
def metadata_generation():
    try:
        app_logger.info("Received /metadata request. Clearing log file...")
        clear_application_log_file() 
        
        event = request.get_json()
        app_logger.info(f"Received /metadata request with payload: {event}") # Log request
        result = metadata_generator(event)
        app_logger.info("Metadata generation completed.") # Log completion

        if isinstance(result, dict):
            # stop_gunicorn()
            return jsonify(result), 200
        elif isinstance(result, str):
            # stop_gunicorn()
            return Response(result, status=200, mimetype='application/json')
    except Exception as e:
        error_message = f"Error in metadata_generation: {e}"
        app_logger.error(error_message, exc_info=True) # Log error with traceback

        # stop_gunicorn()
        return jsonify({"status": "ERROR", "message": error_message}), 500



@app.route("/trigger", methods=["POST"])
def trigger_insights_generation():
    try:
        event = request.get_json()
        app_logger.info(f"Received /trigger request from {request.remote_addr} with payload: {event}") 
        result = insights_generator(event)

        # return "Hello World"
        if isinstance(result, dict): 
            app_logger.info(f"Insights generation status: {result.get('status')}") 
            if result.get("status") == "error":  
                # stop_gunicorn()
                return jsonify({"status": "ERROR", "message": result["message"]}), 500
            elif result.get("status") == "success":
                # stop_gunicorn()
                return jsonify(result), 200 
            else:
                # stop_gunicorn()
                return jsonify(result), 200
        elif isinstance(result, str):
            #  stop_gunicorn()
             return Response(result, status=200, mimetype='application/json')
        else:
            # stop_gunicorn()
            return jsonify({"status": "SUCCESS", "message": "Processed successfully", "data": result}), 200

    except Exception as e:
        error_message = f"Error in trigger_insights_generation: {e}"
        app_logger.error(error_message, exc_info=True)
        # stop_gunicorn()
        return jsonify({"status": "ERROR", "message": error_message}), 500
    

@app.route("/ask", methods=["POST"])
def ask_summary_generation():
    try:
        event = request.get_json()
        app_logger.info(f"Received /ask request with payload: {event}") # Log request
        result = ask_summary_generator(event)
        app_logger.info("Ask summary generation completed.") # Log completion

        if isinstance(result, dict):
            # stop_gunicorn()
            return jsonify(result), 200
        elif isinstance(result, str):
            # stop_gunicorn()
            return Response(result, status=200, mimetype='application/json')
    except Exception as e:
        error_message = f"Error in ask_summary_generation: {e}"
        app_logger.error(error_message, exc_info=True) # Log error with traceback

        # stop_gunicorn()
        return jsonify({"status": "ERROR", "message": error_message}), 500
