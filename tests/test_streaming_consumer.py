import pytest
import json
from unittest.mock import patch, MagicMock
from datetime import timezone, datetime


# ─────────────────────────────────────────
# FLUSH BUFFER
# ─────────────────────────────────────────

def test_flush_buffer_uploads_to_adls():
    """Verifica que flush_buffer llama a upload_to_adls con el contenido correcto."""
    import ingesta.streaming_consumer as consumer
    consumer.buffer = [
        {"product_name": "Hoodie", "total_amount": 79.90, "event_id": "1"}
    ]

    with patch("ingesta.streaming_consumer.upload_to_adls") as mock_upload:
        consumer.flush_buffer()
        mock_upload.assert_called_once()
        call_kwargs = mock_upload.call_args[1]
        assert "ventas_" in call_kwargs["filename"]


def test_flush_buffer_clears_buffer_after_upload():
    """Verifica que flush_buffer vacía el buffer después de escribir."""
    import ingesta.streaming_consumer as consumer
    consumer.buffer = [{"product_name": "Hoodie", "total_amount": 79.90}]

    with patch("ingesta.streaming_consumer.upload_to_adls"):
        consumer.flush_buffer()
        assert consumer.buffer == []


def test_flush_buffer_does_nothing_when_empty():
    """Verifica que flush_buffer no hace nada si el buffer está vacío."""
    import ingesta.streaming_consumer as consumer
    consumer.buffer = []

    with patch("ingesta.streaming_consumer.upload_to_adls") as mock_upload:
        consumer.flush_buffer()
        mock_upload.assert_not_called()


def test_flush_buffer_content_has_required_fields():
    """Verifica que el contenido escrito tiene todos los campos requeridos."""
    import ingesta.streaming_consumer as consumer
    consumer.buffer = [{"product_name": "Hoodie", "total_amount": 79.90}]

    captured_content = {}

    def capture_upload(**kwargs):
        captured_content.update(json.loads(kwargs["content"]))

    with patch("ingesta.streaming_consumer.upload_to_adls", side_effect=capture_upload):
        consumer.flush_buffer()

    assert "batch_date" in captured_content
    assert "batch_timestamp" in captured_content
    assert "total_events" in captured_content
    assert "events" in captured_content
    assert captured_content["total_events"] == 1


# ─────────────────────────────────────────
# ON EVENT
# ─────────────────────────────────────────

def test_on_event_adds_to_buffer():
    """Verifica que on_event añade el evento al buffer."""
    import ingesta.streaming_consumer as consumer
    consumer.buffer = []

    mock_partition_context = MagicMock()
    mock_event = MagicMock()
    mock_event.body_as_str.return_value = json.dumps({
        "product_name": "Hoodie Basic",
        "total_amount": 79.90,
        "event_id": "sale_001"
    })

    with patch("ingesta.streaming_consumer.flush_buffer"):
        consumer.on_event(mock_partition_context, mock_event)
        assert len(consumer.buffer) == 1
        assert consumer.buffer[0]["product_name"] == "Hoodie Basic"


def test_on_event_flushes_when_buffer_full():
    """Verifica que on_event llama a flush_buffer cuando el buffer llega al límite."""
    import ingesta.streaming_consumer as consumer
    consumer.buffer = [{"product_name": "X"} for _ in range(consumer.BUFFER_SIZE - 1)]

    mock_partition_context = MagicMock()
    mock_event = MagicMock()
    mock_event.body_as_str.return_value = json.dumps({
        "product_name": "Hoodie Basic",
        "total_amount": 79.90
    })

    with patch("ingesta.streaming_consumer.flush_buffer") as mock_flush:
        consumer.on_event(mock_partition_context, mock_event)
        mock_flush.assert_called_once()
        mock_partition_context.update_checkpoint.assert_called_once_with(mock_event)


def test_on_event_does_not_flush_when_buffer_not_full():
    """Verifica que on_event no llama a flush_buffer si el buffer no está lleno."""
    import ingesta.streaming_consumer as consumer
    consumer.buffer = []

    mock_partition_context = MagicMock()
    mock_event = MagicMock()
    mock_event.body_as_str.return_value = json.dumps({
        "product_name": "Hoodie Basic",
        "total_amount": 79.90
    })

    with patch("ingesta.streaming_consumer.flush_buffer") as mock_flush:
        consumer.on_event(mock_partition_context, mock_event)
        mock_flush.assert_not_called()


# ─────────────────────────────────────────
# GET EVENTHUBS KAFKA CONFIG
# ─────────────────────────────────────────

def test_get_eventhubs_kafka_config_returns_required_keys():
    """Verifica que la config Kafka tiene todas las claves necesarias."""
    import ingesta.streaming_consumer as consumer

    with patch("ingesta.streaming_consumer.get_secret", side_effect=lambda key: {
        "NAMESPACE_EVENTHUB": "myns.servicebus.windows.net",
        "EVENTHUB_NAME": "sales-events"
    }.get(key, "test")):
        config = consumer.get_eventhubs_kafka_config("fake_conn_str", "sales-events")

    assert "kafka.bootstrap.servers" in config
    assert "subscribe" in config
    assert "kafka.security.protocol" in config
    assert "kafka.sasl.mechanism" in config
    assert "kafka.sasl.jaas.config" in config


def test_get_eventhubs_kafka_config_uses_correct_namespace():
    """Verifica que el bootstrap server usa el namespace correcto."""
    import ingesta.streaming_consumer as consumer

    with patch("ingesta.streaming_consumer.get_secret", side_effect=lambda key: {
        "NAMESPACE_EVENTHUB": "myns.servicebus.windows.net",
        "EVENTHUB_NAME": "sales-events"
    }.get(key, "test")):
        config = consumer.get_eventhubs_kafka_config("fake_conn_str", "sales-events")

    assert config["kafka.bootstrap.servers"] == "myns.servicebus.windows.net:9093"


def test_get_eventhubs_kafka_config_raises_without_topic():
    """Verifica que lanza error si no hay nombre de Event Hub."""
    import ingesta.streaming_consumer as consumer

    with patch("ingesta.streaming_consumer.get_secret", side_effect=lambda key: {
        "NAMESPACE_EVENTHUB": "myns.servicebus.windows.net",
        "EVENTHUB_NAME": None
    }.get(key)):
        with pytest.raises(ValueError):
            consumer.get_eventhubs_kafka_config("fake_conn_str", None)


# ─────────────────────────────────────────
# MAKE WRITE BATCH
# ─────────────────────────────────────────

def test_make_write_batch_skips_empty_batch():
    """Verifica que write_batch no hace nada con un batch vacío."""
    import ingesta.streaming_consumer as consumer

    write_batch = consumer.make_write_batch(
        container="c", account_name="a", account_key="k",
        layer="landing", source="ventas"
    )

    mock_df = MagicMock()
    mock_df.isEmpty.return_value = True

    with patch("ingesta.streaming_consumer.upload_to_adls") as mock_upload:
        write_batch(mock_df, 0)
        mock_upload.assert_not_called()


def test_make_write_batch_uploads_events():
    """Verifica que write_batch sube los eventos cuando el batch no está vacío."""
    import ingesta.streaming_consumer as consumer

    write_batch = consumer.make_write_batch(
        container="c", account_name="a", account_key="k",
        layer="landing", source="ventas"
    )

    mock_df = MagicMock()
    mock_df.isEmpty.return_value = False
    mock_df.collect.return_value = [
        MagicMock(asDict=lambda: {"product_name": "Hoodie", "total_amount": 79.90})
    ]

    with patch("ingesta.streaming_consumer.upload_to_adls") as mock_upload:
        write_batch(mock_df, 1)
        mock_upload.assert_called_once()
        call_kwargs = mock_upload.call_args[1]
        assert call_kwargs["container"] == "c"
        assert call_kwargs["account_name"] == "a"
        assert call_kwargs["layer"] == "landing"
        assert call_kwargs["folder"] == "ventas"