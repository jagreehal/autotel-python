"""Flask example with autotel."""

from flask import Flask, jsonify

from autotel.integrations.flask import init_autotel

app = Flask(__name__)

# Initialize autotel
init_autotel(app, service="flask-example")


@app.route("/")
def read_root() -> None:
    """Root endpoint."""
    return jsonify({"message": "Hello World"})


@app.route("/users/<int:user_id>")
def get_user(user_id) -> None:
    """Get user by ID."""
    return jsonify({"user_id": user_id, "name": "John Doe"})


@app.route("/users", methods=["POST"])
def create_user() -> None:
    """Create a new user."""
    from flask import request

    return jsonify({"id": 123, **request.json})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
