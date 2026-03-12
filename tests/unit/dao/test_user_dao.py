"""
Unit tests for UserDao.

Tests CRUD operations and validation logic for the users table.
"""
import pytest
from datetime import datetime
from unittest.mock import patch
from app.domains.auth.dao.user_dao import UserDao, UserRow


@pytest.mark.unit
class TestUserDao:
    """Tests for UserDao CRUD operations."""

    @pytest.fixture
    def user_dao(self):
        """Return a UserDao instance."""
        return UserDao()

    def test_username_exists_true(self, user_dao, db_connection_sync):
        """Test username_exists returns True when user exists."""
        # Arrange
        username = "testuser_exists"
        from sqlalchemy import text
        db_connection_sync.execute(
            text("INSERT INTO users (username, email, hashed_password, is_active, created_at) VALUES (:u, :e, :p, 1, :c)"),
            {"u": username, "e": "test@example.com", "p": "hashed_pw", "c": datetime.utcnow()}
        )
        db_connection_sync.commit()

        # Patch the connection to use our test database
        with patch('app.infrastructure.db.connections.get_tradermate_connection', return_value=db_connection_sync):
            # Act
            result = user_dao.username_exists(username)
            # Assert
            assert result is True

    def test_username_exists_false_for_missing(self, user_dao, db_connection_sync):
        """Test username_exists returns False when user does not exist."""
        # Patch the connection to use test database (no users inserted)
        with patch('app.infrastructure.db.connections.get_tradermate_connection', return_value=db_connection_sync):
            # Act
            result = user_dao.username_exists("nonexistent_user_xyz")
            # Assert
            assert result is False

    def test_email_exists_true(self, user_dao, db_connection_sync):
        """Test email_exists returns True when email exists."""
        # Arrange
        email = "test_exists@example.com"
        from sqlalchemy import text
        db_connection_sync.execute(
            text("INSERT INTO users (username, email, hashed_password, is_active, created_at) VALUES (:u, :e, :p, 1, :c)"),
            {"u": "test_email_user", "e": email, "p": "hashed_pw", "c": datetime.utcnow()}
        )
        db_connection_sync.commit()

        # Patch the connection to use our test database
        with patch('app.infrastructure.db.connections.get_tradermate_connection', return_value=db_connection_sync):
            # Act
            result = user_dao.email_exists(email)
            # Assert
            assert result is True

    def test_email_exists_false_for_none(self, user_dao):
        """Test email_exists returns False for None/empty email."""
        # Act
        result = user_dao.email_exists(None)
        result_empty = user_dao.email_exists("")
        # Assert
        assert result is False
        assert result_empty is False

    @pytest.mark.skip(reason="Requires full DB schema with constraints")
    def test_insert_user_success(self, user_dao, db_connection_sync):
        """Test insert_user creates a new user and returns ID."""
        # Arrange
        username = "new_test_user"
        email = "new@example.com"
        hashed_pw = "hashed_password_123"
        created_at = datetime.utcnow()

        # Patch the connection to use our test database
        with patch('app.infrastructure.db.connections.get_tradermate_connection', return_value=db_connection_sync):
            # Act
            user_id = user_dao.insert_user(
                username=username,
                email=email,
                hashed_password=hashed_pw,
                created_at=created_at,
                must_change_password=False
            )

            # Assert
            assert isinstance(user_id, int)
            assert user_id > 0

            # Verify insertion
            row = db_connection_sync.execute(
                text("SELECT username, email, is_active FROM users WHERE id = :id"),
                {"id": user_id}
            ).fetchone()
            assert row is not None
            assert row.username == username
            assert row.email == email
            assert row.is_active is True
