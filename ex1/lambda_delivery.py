import json
import time
import random

insults = ["tonto", "bobo", "puta", "idiota", "cabron"]

def lambda_handler(event, context): # filter_text
    frase = event.get('frase')
    print (f"Received phrase: {frase}")

    for insult in insults:
        frase = frase.replace(insult, "CENSORED")

    print(f"[InsultFilter] Filtered phrase: {frase}")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "Phrase delivered",
            "frase": frase
        })
    }
