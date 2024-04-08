import torch
import torch.nn.functional as F
from transformers import AutoModelForSequenceClassification, AutoTokenizer

class sentiment:
    def __init__(self):
        torch.manual_seed(694201337)

        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

        self.model_name = 'yiyanghkust/finbert-tone'

        # Load the FinBERT tokenizer and model
        self.tokenizer = AutoTokenizer.from_pretrained(self.model_name)
        self.model = AutoModelForSequenceClassification.from_pretrained(self.model_name).to(self.device)

        # Set the maximum sequence length (including special tokens)
        self.max_length = self.model.config.max_position_embeddings

    def tokenize_and_truncate(self, text):
        # Tokenize the text
        tokens = self.tokenizer.tokenize(text)

        # Truncate if the length exceeds the maximum length
        if len(tokens) > self.max_length - 2:
            tokens = tokens[:self.max_length - 2]

        return tokens

    def score_text(self, text):
        # Tokenize and truncate the article
        tokens = self.tokenize_and_truncate(text)

        # Convert tokens to input IDs
        input_ids = self.tokenizer.convert_tokens_to_ids(tokens)

        # Create attention mask
        attention_mask = [1] * len(input_ids)

        # Pad sequences to the maximum length
        padding_length = self.max_length - len(input_ids)
        input_ids += [self.tokenizer.pad_token_id] * padding_length
        attention_mask += [0] * padding_length

        # Convert input lists to tensors
        input_ids = torch.tensor(input_ids).unsqueeze(0).to(self.device)
        attention_mask = torch.tensor(attention_mask).unsqueeze(0).to(self.device)

        # Set the model to evaluation mode
        self.model.eval()

        # Disable gradient calculation
        with torch.no_grad():
            # Forward pass
            outputs = self.model(input_ids=input_ids, attention_mask=attention_mask)

        sentiment_probs = F.softmax(outputs.logits, dim=1)

        return {"neg": sentiment_probs[0][2].item(), "neu": sentiment_probs[0][0].item(), "pos": sentiment_probs[0][1].item()}


