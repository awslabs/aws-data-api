import boto3
import os
import argparse
import time
from chalicelib.exceptions import InvalidArgumentsException
from botocore.exceptions import ClientError

RAW_LINES = "RawLines"
KEY_VALUES = "KeyValues"
ENTITIES = "Entities"
LANGUAGE = "LanguageCode"
SENTIMENT = "Sentiment"
KEY_PHRASES = "KeyPhrases"
PAGES = "Pages"
BLOCK_TYPE = "BlockType"


# class to implement the metadata extraction toolchain of translate->textract->comprehend.
class Understander:
    region = None
    textract_client = None
    comprehend_client = None
    s3_resource = None

    def __init__(self, region):
        self.region = region

        # setup clients
        self.textract_client = boto3.client("textract", region_name=region)
        self.comprehend_client = boto3.client("comprehend", region_name=region)
        self.s3_resource = boto3.resource("s3")

    def _textract(self, bucket, prefix):
        doc = {
            'S3Object': {
                'Bucket': bucket,
                'Name': prefix
            }
        }

        analysis_job = self.textract_client.start_document_analysis(
            DocumentLocation=doc,
            FeatureTypes=[
                'FORMS',
            ]
        )
        job = analysis_job["JobId"]

        finished = False
        doc_analysis = None
        while not finished:
            doc_analysis = self.textract_client.get_document_analysis(JobId=job)
            if 'JobStatus' in doc_analysis:
                status = doc_analysis.get('JobStatus')

                if status == "SUCCEEDED":
                    finished = True
                else:
                    time.sleep(.5)

        # build the raw text lines and word map
        words = {}
        for b in doc_analysis.get('Blocks', []):
            # index all the words for extraction in the KEY_VALUE_SET processing
            if b.get(BLOCK_TYPE) == "WORD":
                words[b.get("Id")] = b.get("Text")

        # now build the key/value set from word graph
        concat_strings = {}
        text_lines = []
        kv = {}
        for b in doc_analysis.get('Blocks', []):
            block_id = b.get("Id")

            t = "EntityTypes" in b and b.get("EntityTypes")[0]

            if b.get(BLOCK_TYPE) == "KEY_VALUE_SET" and any([v in t for v in ['VALUE', 'KEY']]):
                value_tokens = []
                for r in b.get("Relationships", []):
                    if r.get("Type") == 'CHILD':
                        for i in r.get("Ids", []):
                            # extract the word from the word index
                            value_tokens.append(words.get(i))

                        # the value map is all the words from the value relationship separated by spaces
                        concat_strings[block_id] = " ".join(value_tokens)
                    elif r.get("Type") == 'VALUE' and t == 'KEY':
                        # enter this value into the KV map
                        kv[b.get("Id")] = r.get("Ids")[0]

            if b.get(BLOCK_TYPE) == 'LINE':
                text_lines.append(b.get("Text"))

        # finally, generate the KV map as a set of concatenated strings
        key_value_set = {}
        for k, v in kv.items():
            key_value_set[concat_strings.get(k)] = concat_strings.get(v)

        return {
            PAGES: doc_analysis.get('DocumentMetadata').get('Pages'),
            RAW_LINES: text_lines,
            KEY_VALUES: key_value_set
        }

    def _extract_entities(self, raw_text, lang):
        entity_response = self.comprehend_client.detect_entities(
            Text=raw_text,
            LanguageCode=lang
        )

        entities = {}
        for e in [] if "Entities" not in entity_response else entity_response["Entities"]:
            if e.get("Type") not in entities:
                entities[e.get("Type")] = [e.get("Text")]
            else:
                entities[e.get("Type")].append(e.get("Text"))

        return entities

    def _get_dominant_language(self, raw_text):
        response = self.comprehend_client.detect_dominant_language(
            Text=raw_text
        )

        # create a dict indexed by score
        indexed_lang = {}
        if response is not None and "Languages" in response:
            for r in response.get("Languages"):
                indexed_lang[r.get("Score")] = r.get(LANGUAGE)

        top_score = sorted(indexed_lang.keys())[0]

        return indexed_lang.get(top_score)

    def _extract_sentiment(self, raw_text, lang):
        sentiment_response = self.comprehend_client.detect_sentiment(
            Text=raw_text,
            LanguageCode=lang
        )

        if sentiment_response is not None and "SentimentScore" in sentiment_response:
            return sentiment_response.get("SentimentScore")
        else:
            return {}

    def _get_phrases(self, raw_text, lang):
        phrases_response = self.comprehend_client.detect_key_phrases(
            Text=raw_text,
            LanguageCode=lang
        )

        phrases = []
        if phrases_response is not None and "KeyPhrases" in phrases_response:
            for p in phrases_response.get("KeyPhrases", []):
                phrases.append(p.get("Text"))

        return phrases

    def _comprehend(self, raw_text):
        lang = self._get_dominant_language(raw_text)

        return {
            ENTITIES: self._extract_entities(raw_text, lang),
            LANGUAGE: lang,
            SENTIMENT: self._extract_sentiment(raw_text, lang),
            KEY_PHRASES: self._get_phrases(raw_text, lang)
        }

    # main method to implement the understanding toolchain against an S3 object
    #
    # return struct is:
    # {
    #     "Pages": <page count>, (Number of pages in the document - only relevant for PDF)
    #     "RawLines": <string>, (Space separated capture of all raw text from the document)
    #     "KeyValues": {}, (Key/Value map of elements from the document)
    #     "Entities": {}, (Map of Entities detected in the document - per https://docs.aws.amazon.com/comprehend/latest/dg/how-entities.html)
    #     "LanguageCode": <iso language code>,
    #     "Sentiment": {}, (Sentiment value and confidence score for that value)
    #     "KeyPhrases": [], (List of key phrases found in the document)
    # }
    def understand(self, prefix):
        require_s3 = "Prefix must be an S3 Object"
        if prefix is None:
            raise InvalidArgumentsException(require_s3)
        else:
            s3_pref = "s3://"
            if not prefix[0:4] != s3_pref:
                raise InvalidArgumentsException(require_s3)
            else:
                prefix_tokens = prefix.replace(s3_pref, "").split("/")
                bucket = prefix_tokens[0]
                object_prefix = "/".join(prefix_tokens[1:])

                try:
                    object = self.s3_resource.Object(bucket, object_prefix)

                    # validate supported mime types
                    if not any([x in object.content_type for x in
                                ["image/jpeg", "image/pjpeg", "image/png", "application/pdf"]]):
                        raise InvalidArgumentsException("Understander only supports JPG, PNG, and PDF")
                    else:
                        response = {}

                        # textract the contents of the target file
                        textraction = self._textract(bucket, object_prefix)
                        response.update(textraction)

                        # run comprehend entity extraction
                        response.update(self._comprehend(raw_text=" ".join(textraction.get(RAW_LINES))))

                        return response
                except ClientError as e:
                    if e.response['Error']['Code'] == '404':
                        raise InvalidArgumentsException("Object Not Found")
                    else:
                        raise e


if __name__ == "__main__":
    u = Understander(os.getenv('AWS_REGION'))

    parser = argparse.ArgumentParser()
    parser.add_argument('--prefix')

    args = parser.parse_args()
    response = u.understand(prefix=args.prefix)
    print(response)
