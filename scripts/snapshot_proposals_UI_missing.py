from src.db.sql import SQL

SPACE_ID = 'lido-snapshot.eth'

sql = SQL()
data = sql.read_data(f"SELECT * FROM proposals where space_id = '{SPACE_ID}'")

missing = []

not_missing_titles = set()
# File retrived by running the following JavaScript code on browser
# (function () {
#     // Select all matching h3 elements
#     const elements = document.querySelectorAll('h3.text-\\[21px\\].inline.\\[overflow-wrap\\:anywhere\\].min-w-0');
#
#     // Extract text content from each element
#     const textContent = Array.from(elements).map(el => el.textContent.trim()).join("\n");
#
#     // Create a Blob with the text data
#     const blob = new Blob([textContent], { type: 'text/plain' });
#
#     // Create a temporary link element
#     const link = document.createElement('a');
#     link.href = URL.createObjectURL(blob);
#     link.download = "h3_texts.txt"; // File name
#
#     // Trigger the download
#     document.body.appendChild(link);
#     link.click();
#
#     // Cleanup
#     document.body.removeChild(link);
#     URL.revokeObjectURL(link.href);
# })();
with open(f"{SPACE_ID}_UI.txt", "r") as f:
    not_missing_titles.update(map(lambda x: x.strip(), f.readlines()))

for item in data:
    if item[1] not in not_missing_titles:
        missing.append(item)

for item in missing:
    print(item[0])