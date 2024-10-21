import json

input = """{
   "title":"Republic of the Rif",
   "url":"https://en.wikipedia.org/wiki/Republic_of_the_Rif",
   "content":"The Republic of the Rif (Arabic: جمهورية الريف Jumhūriyyatu r-Rīf) was a confederate republic in the Rif, Morocco, that existed between 1921 and 1926. It was created in September 1921, when a coalition of Rifians and Jebala led by Abd el-Krim revolted in the Rif War against the Spanish protectorate in Morocco. The French would intervene on the side of Spain in the later stages of the conflict. A protracted struggle for independence killed many Rifians and Spanish–French soldiers, and witnessed the use of chemical weapons by the Spanish army—their first widespread deployment since the end of the World War I. The eventual Spanish–French victory was owed to the technological and manpower advantages despite their lack of morale and coherence.  Following the war's end, the Republic was ultimately dissolved in 1926."
}"""

payload = json.loads(input)
print (payload)

payload["$vectorize"] = "abc"
payload["metadata"] = {"source": "Pulsar"}

print (json.dumps(payload))