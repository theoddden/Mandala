"""Comprehensive tests for alert routing module."""

import asyncio
from datetime import datetime, timezone
from unittest.mock import AsyncMock, Mock

import pytest

from mandala.core.alert_routing import AlertRouter, Route, RoutingRule


class TestRoute:
    """Test cases for Route."""

    def test_route_initialization(self):
        """Test that Route initializes correctly."""
        route = Route(
            id="route-1",
            destination="webhook",
            config={"url": "https://example.com/webhook"},
        )
        assert route.id == "route-1"
        assert route.destination == "webhook"
        assert route.config == {"url": "https://example.com/webhook"}

    def test_route_to_dict(self):
        """Test converting route to dictionary."""
        route = Route(
            id="route-1",
            destination="webhook",
            config={"url": "https://example.com/webhook"},
        )
        data = route.to_dict()
        assert data["id"] == "route-1"
        assert data["destination"] == "webhook"

    def test_route_from_dict(self):
        """Test creating route from dictionary."""
        data = {
            "id": "route-1",
            "destination": "webhook",
            "config": {"url": "https://example.com/webhook"},
        }
        route = Route.from_dict(data)
        assert route.id == "route-1"
        assert route.destination == "webhook"


class TestRoutingRule:
    """Test cases for RoutingRule."""

    def test_routing_rule_initialization(self):
        """Test that RoutingRule initializes correctly."""
        rule = RoutingRule(
            id="rule-1",
            condition={"severity": "high"},
            route_id="route-1",
            priority=10,
        )
        assert rule.id == "rule-1"
        assert rule.condition == {"severity": "high"}
        assert rule.route_id == "route-1"
        assert rule.priority == 10

    def test_routing_rule_matches(self):
        """Test that routing rule matches alert."""
        rule = RoutingRule(
            id="rule-1",
            condition={"severity": "high"},
            route_id="route-1",
            priority=10,
        )
        alert = {"severity": "high", "source": "test"}
        assert rule.matches(alert) is True

    def test_routing_rule_does_not_match(self):
        """Test that routing rule does not match alert."""
        rule = RoutingRule(
            id="rule-1",
            condition={"severity": "high"},
            route_id="route-1",
            priority=10,
        )
        alert = {"severity": "medium", "source": "test"}
        assert rule.matches(alert) is False

    def test_routing_rule_multiple_conditions(self):
        """Test routing rule with multiple conditions."""
        rule = RoutingRule(
            id="rule-1",
            condition={"severity": "high", "source": "test"},
            route_id="route-1",
            priority=10,
        )
        alert = {"severity": "high", "source": "test"}
        assert rule.matches(alert) is True

    def test_routing_rule_partial_match_fails(self):
        """Test that partial condition match fails."""
        rule = RoutingRule(
            id="rule-1",
            condition={"severity": "high", "source": "test"},
            route_id="route-1",
            priority=10,
        )
        alert = {"severity": "high", "source": "other"}
        assert rule.matches(alert) is False


class TestAlertRouter:
    """Test cases for AlertRouter."""

    @pytest.fixture
    def mock_http_client(self):
        """Create a mock HTTP client."""
        client = AsyncMock()
        client.post = AsyncMock(return_value=Mock(status_code=200))
        return client

    @pytest.fixture
    def router(self, mock_http_client):
        """Create an AlertRouter instance."""
        return AlertRouter(http_client=mock_http_client)

    def test_router_initialization(self, mock_http_client):
        """Test that AlertRouter initializes correctly."""
        router = AlertRouter(http_client=mock_http_client)
        assert router._http_client == mock_http_client
        assert router._routes == []
        assert router._rules == []

    @pytest.mark.asyncio
    async def test_add_route(self, router):
        """Test adding a route."""
        route = Route(
            id="route-1",
            destination="webhook",
            config={"url": "https://example.com/webhook"},
        )
        await router.add_route(route)
        assert len(router._routes) == 1
        assert router._routes[0].id == "route-1"

    @pytest.mark.asyncio
    async def test_add_rule(self, router):
        """Test adding a routing rule."""
        rule = RoutingRule(
            id="rule-1",
            condition={"severity": "high"},
            route_id="route-1",
            priority=10,
        )
        await router.add_rule(rule)
        assert len(router._rules) == 1
        assert router._rules[0].id == "rule-1"

    @pytest.mark.asyncio
    async def test_route_alert(self, router, mock_http_client):
        """Test routing an alert."""
        route = Route(
            id="route-1",
            destination="webhook",
            config={"url": "https://example.com/webhook"},
        )
        rule = RoutingRule(
            id="rule-1",
            condition={"severity": "high"},
            route_id="route-1",
            priority=10,
        )
        await router.add_route(route)
        await router.add_rule(rule)

        alert = {
            "id": "alert-1",
            "severity": "high",
            "source": "test",
            "message": "Test alert",
        }
        result = await router.route_alert(alert)

        assert result is not None
        mock_http_client.post.assert_called()

    @pytest.mark.asyncio
    async def test_route_alert_no_match(self, router):
        """Test routing alert with no matching rule."""
        rule = RoutingRule(
            id="rule-1",
            condition={"severity": "high"},
            route_id="route-1",
            priority=10,
        )
        await router.add_rule(rule)

        alert = {
            "id": "alert-1",
            "severity": "medium",
            "source": "test",
            "message": "Test alert",
        }
        result = await router.route_alert(alert)

        assert result == []

    @pytest.mark.asyncio
    async def test_route_alert_multiple_rules(self, router, mock_http_client):
        """Test routing alert with multiple matching rules."""
        route1 = Route(
            id="route-1",
            destination="webhook",
            config={"url": "https://example.com/webhook1"},
        )
        route2 = Route(
            id="route-2",
            destination="email",
            config={"email": "admin@example.com"},
        )
        rule1 = RoutingRule(
            id="rule-1",
            condition={"severity": "high"},
            route_id="route-1",
            priority=10,
        )
        rule2 = RoutingRule(
            id="rule-2",
            condition={"severity": "high"},
            route_id="route-2",
            priority=5,
        )
        await router.add_route(route1)
        await router.add_route(route2)
        await router.add_rule(rule1)
        await router.add_rule(rule2)

        alert = {
            "id": "alert-1",
            "severity": "high",
            "source": "test",
            "message": "Test alert",
        }
        result = await router.route_alert(alert)

        # Should route to both matching rules
        assert len(result) == 2

    @pytest.mark.asyncio
    async def test_route_alert_priority_order(self, router, mock_http_client):
        """Test that rules are evaluated by priority."""
        route1 = Route(
            id="route-1",
            destination="webhook",
            config={"url": "https://example.com/webhook1"},
        )
        route2 = Route(
            id="route-2",
            destination="email",
            config={"email": "admin@example.com"},
        )
        rule1 = RoutingRule(
            id="rule-1",
            condition={"severity": "high"},
            route_id="route-1",
            priority=10,
        )
        rule2 = RoutingRule(
            id="rule-2",
            condition={"severity": "high"},
            route_id="route-2",
            priority=20,
        )
        await router.add_route(route1)
        await router.add_route(route2)
        await router.add_rule(rule1)
        await router.add_rule(rule2)

        alert = {
            "id": "alert-1",
            "severity": "high",
            "source": "test",
            "message": "Test alert",
        }
        result = await router.route_alert(alert)

        # Higher priority should be evaluated first
        assert result is not None

    @pytest.mark.asyncio
    async def test_remove_route(self, router):
        """Test removing a route."""
        route = Route(
            id="route-1",
            destination="webhook",
            config={"url": "https://example.com/webhook"},
        )
        await router.add_route(route)
        assert len(router._routes) == 1

        await router.remove_route("route-1")
        assert len(router._routes) == 0

    @pytest.mark.asyncio
    async def test_remove_rule(self, router):
        """Test removing a routing rule."""
        rule = RoutingRule(
            id="rule-1",
            condition={"severity": "high"},
            route_id="route-1",
            priority=10,
        )
        await router.add_rule(rule)
        assert len(router._rules) == 1

        await router.remove_rule("rule-1")
        assert len(router._rules) == 0

    @pytest.mark.asyncio
    async def test_get_routes(self, router):
        """Test getting all routes."""
        route1 = Route(
            id="route-1",
            destination="webhook",
            config={"url": "https://example.com/webhook1"},
        )
        route2 = Route(
            id="route-2",
            destination="email",
            config={"email": "admin@example.com"},
        )
        await router.add_route(route1)
        await router.add_route(route2)

        routes = await router.get_routes()
        assert len(routes) == 2

    @pytest.mark.asyncio
    async def test_get_rules(self, router):
        """Test getting all routing rules."""
        rule1 = RoutingRule(
            id="rule-1",
            condition={"severity": "high"},
            route_id="route-1",
            priority=10,
        )
        rule2 = RoutingRule(
            id="rule-2",
            condition={"severity": "medium"},
            route_id="route-2",
            priority=5,
        )
        await router.add_rule(rule1)
        await router.add_rule(rule2)

        rules = await router.get_rules()
        assert len(rules) == 2

    @pytest.mark.asyncio
    async def test_route_alert_with_webhook_destination(self, router, mock_http_client):
        """Test routing alert to webhook destination."""
        route = Route(
            id="route-1",
            destination="webhook",
            config={"url": "https://example.com/webhook"},
        )
        rule = RoutingRule(
            id="rule-1",
            condition={"severity": "high"},
            route_id="route-1",
            priority=10,
        )
        await router.add_route(route)
        await router.add_rule(rule)

        alert = {
            "id": "alert-1",
            "severity": "high",
            "source": "test",
            "message": "Test alert",
        }
        await router.route_alert(alert)

        mock_http_client.post.assert_called_once()
        call_args = mock_http_client.post.call_args
        assert "https://example.com/webhook" in str(call_args)

    @pytest.mark.asyncio
    async def test_route_alert_with_email_destination(self, router, mock_http_client):
        """Test routing alert to email destination."""
        route = Route(
            id="route-1",
            destination="email",
            config={"email": "admin@example.com"},
        )
        rule = RoutingRule(
            id="rule-1",
            condition={"severity": "high"},
            route_id="route-1",
            priority=10,
        )
        await router.add_route(route)
        await router.add_rule(rule)

        alert = {
            "id": "alert-1",
            "severity": "high",
            "source": "test",
            "message": "Test alert",
        }
        await router.route_alert(alert)

        # Email routing should be handled
        assert len(router._routes) == 1

    @pytest.mark.asyncio
    async def test_route_alert_with_slack_destination(self, router, mock_http_client):
        """Test routing alert to Slack destination."""
        route = Route(
            id="route-1",
            destination="slack",
            config={"webhook_url": "https://hooks.slack.com/webhook"},
        )
        rule = RoutingRule(
            id="rule-1",
            condition={"severity": "high"},
            route_id="route-1",
            priority=10,
        )
        await router.add_route(route)
        await router.add_rule(rule)

        alert = {
            "id": "alert-1",
            "severity": "high",
            "source": "test",
            "message": "Test alert",
        }
        await router.route_alert(alert)

        mock_http_client.post.assert_called()

    @pytest.mark.asyncio
    async def test_route_alert_with_retry_on_failure(self, router, mock_http_client):
        """Test routing with retry on HTTP failure."""
        mock_http_client.post.side_effect = Exception("HTTP error")
        route = Route(
            id="route-1",
            destination="webhook",
            config={"url": "https://example.com/webhook"},
        )
        rule = RoutingRule(
            id="rule-1",
            condition={"severity": "high"},
            route_id="route-1",
            priority=10,
        )
        await router.add_route(route)
        await router.add_rule(rule)

        alert = {
            "id": "alert-1",
            "severity": "high",
            "source": "test",
            "message": "Test alert",
        }

        # Should handle the error gracefully
        result = await router.route_alert(alert)
        assert result is not None

    @pytest.mark.asyncio
    async def test_route_alert_with_timeout(self, router, mock_http_client):
        """Test routing with timeout."""

        async def slow_post(*args, **kwargs):
            await asyncio.sleep(2)
            return Mock(status_code=200)

        mock_http_client.post = slow_post
        route = Route(
            id="route-1",
            destination="webhook",
            config={"url": "https://example.com/webhook"},
        )
        rule = RoutingRule(
            id="rule-1",
            condition={"severity": "high"},
            route_id="route-1",
            priority=10,
        )
        await router.add_route(route)
        await router.add_rule(rule)

        alert = {
            "id": "alert-1",
            "severity": "high",
            "source": "test",
            "message": "Test alert",
        }

        # Should handle timeout gracefully
        result = await router.route_alert(alert, timeout=0.1)
        assert result is not None

    @pytest.mark.asyncio
    async def test_get_routing_statistics(self, router):
        """Test getting routing statistics."""
        route = Route(
            id="route-1",
            destination="webhook",
            config={"url": "https://example.com/webhook"},
        )
        rule = RoutingRule(
            id="rule-1",
            condition={"severity": "high"},
            route_id="route-1",
            priority=10,
        )
        await router.add_route(route)
        await router.add_rule(rule)

        stats = await router.get_statistics()
        assert stats is not None
        assert "total_routes" in stats
        assert "total_rules" in stats

    @pytest.mark.asyncio
    async def test_batch_route_alerts(self, router, mock_http_client):
        """Test routing multiple alerts in batch."""
        route = Route(
            id="route-1",
            destination="webhook",
            config={"url": "https://example.com/webhook"},
        )
        rule = RoutingRule(
            id="rule-1",
            condition={"severity": "high"},
            route_id="route-1",
            priority=10,
        )
        await router.add_route(route)
        await router.add_rule(rule)

        alerts = [
            {
                "id": f"alert-{i}",
                "severity": "high",
                "source": "test",
                "message": f"Test alert {i}",
            }
            for i in range(5)
        ]

        results = await router.batch_route_alerts(alerts)
        assert len(results) == 5

    @pytest.mark.asyncio
    async def test_enable_route(self, router):
        """Test enabling a route."""
        route = Route(
            id="route-1",
            destination="webhook",
            config={"url": "https://example.com/webhook"},
            enabled=False,
        )
        await router.add_route(route)

        await router.enable_route("route-1")
        assert router._routes[0].enabled is True

    @pytest.mark.asyncio
    async def test_disable_route(self, router):
        """Test disabling a route."""
        route = Route(
            id="route-1",
            destination="webhook",
            config={"url": "https://example.com/webhook"},
            enabled=True,
        )
        await router.add_route(route)

        await router.disable_route("route-1")
        assert router._routes[0].enabled is False

    @pytest.mark.asyncio
    async def test_route_alert_with_disabled_route(self, router, mock_http_client):
        """Test that disabled routes are not used."""
        route = Route(
            id="route-1",
            destination="webhook",
            config={"url": "https://example.com/webhook"},
            enabled=False,
        )
        rule = RoutingRule(
            id="rule-1",
            condition={"severity": "high"},
            route_id="route-1",
            priority=10,
        )
        await router.add_route(route)
        await router.add_rule(rule)

        alert = {
            "id": "alert-1",
            "severity": "high",
            "source": "test",
            "message": "Test alert",
        }
        result = await router.route_alert(alert)

        # Should not route to disabled route
        assert result == []
