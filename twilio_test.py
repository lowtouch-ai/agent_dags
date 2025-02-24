import os
from twilio.rest import Client

class TwilioVoiceCall:
    """
    Class to handle automated voice calls using Twilio API.
    """

    def __init__(self):
        """Initialize Twilio client using environment variables."""
        self.account_sid = os.getenv("TWILIO_ACCOUNT_SID")
        self.auth_token = os.getenv("TWILIO_AUTH_TOKEN")
        self.twilio_phone = os.getenv("TWILIO_PHONE_NUMBER")

        if not all([self.account_sid, self.auth_token, self.twilio_phone]):
            raise ValueError("Twilio credentials are missing. Check environment variables.")

        self.client = Client(self.account_sid, self.auth_token)

    def send_call(self, message: str, phone_number: str, need_ack: bool = False) -> str:
        """
        Send a voice call with a custom message.

        Args:
            message (str): The message to be spoken during the call.
            phone_number (str): Recipient's phone number.
            need_ack (bool): If True, waits for acknowledgment (simulated).

        Returns:
            str: Call status message.
        """
        if not message or not phone_number:
            raise ValueError("Both 'message' and 'phone_number' are required.")

        call = self.client.calls.create(
    to=phone_number,
    from_=self.twilio_phone,
    twiml=f'<Response><Say>{message}</Say></Response>',
    status_callback="https://yourserver.com/call_status",  # Webhook for status updates
    status_callback_event=["completed"]
)

        if need_ack:
            return f"Call initiated. Call SID: {call.sid}. Awaiting acknowledgment..."
        return f"Call initiated successfully. Call SID: {call.sid}"


if __name__ == "__main__":
    # Example Usage (Standalone)
    twilio_call = TwilioVoiceCall()
    phone_number = "+917025693146"  # Replace with the recipient's number
    message = "Hello, this is a test call from your friendly bot. Did you receive it?"
    
    response = twilio_call.send_call(message, phone_number, need_ack=True)
    print(response)
