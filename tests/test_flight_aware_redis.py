import py1090

from flight_aware_redis import to_record


def test_to_record():
# Create a mock flight message
    message = py1090.Message.from_string("MSG,3,111,11111,3C49CC,111111,2015/05/01,17:06:55.370,2015/05/01,17:06:55.326,,24400,,,50.65931,6.67709,,,,,,0")

    # Convert the message to a record
    record = to_record(message)

    # Check if the record contains the expected fields
    assert record['hexident'] == "3C49CC"
    assert record['latitude'] == 50.65931
    assert record['longitude'] == 6.67709
    assert record['altitude'] == 24400
    #assert record['callsign'] == "TEST123"
    assert record["message_type"] == "MSG"
