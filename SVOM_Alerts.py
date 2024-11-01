import datetime as dt
from pyGCN import GCNListener
from voeventparse import loads as parse_voevent
from astropy.time import Time

# Define a function to handle the alert data
def handle_alert(packet):
    """Process the VOEvent alert packet for recent GRBs"""
    voevent = parse_voevent(packet)  # Parse the VOEvent packet
    
    # Retrieve the event time from the VOEvent
    event_time_str = voevent.Who.Date
    event_time = Time(event_time_str)  # Convert to astropy Time object
    
    # Calculate the time difference from now
    current_time = Time.now()
    time_diff = (current_time - event_time).to('day').value  # Difference in days
    
    # Only process events from the last day
    if time_diff <= 1:
        # Extract relevant details
        try:
            author = voevent.Who.Author.shortName.text
            ra = voevent.WhereWhen.ObsDataLocation.ObservationLocation.AstroCoords.Position2D.Value2.C1
            dec = voevent.WhereWhen.ObsDataLocation.ObservationLocation.AstroCoords.Position2D.Value2.C2
            error_radius = voevent.WhereWhen.ObsDataLocation.ObservationLocation.AstroCoords.Position2D.Error2Radius
            print(f"New GRB Alert from {author}:\n RA: {ra}, Dec: {dec}, Error Radius: {error_radius}")
        except AttributeError:
            print("Alert received but missing some information.")
    else:
        print("Received alert is older than one day. Ignoring.")

def run_listener_once():
    """Connect to the GCN server, retrieve alerts, and disconnect."""
    listener = gcn.listen(host='voevent.svom.eu', port=8099, handler=handler, iamalive_timeout=100.0, max_reconnect_timeout=1)
    #GCNListener(handle=handle_alert)  # Setup listener with handler
    try:
        listener.run(timeout=30)  # Run listener for 30 seconds, then disconnect
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        print("Finished checking for alerts.")


# Execute the listener in "run-once" mode
if __name__ == "__main__":
    print("Checking for GRB alerts from the last day...")
    run_listener_once()
