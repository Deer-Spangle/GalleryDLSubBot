from PIL import Image


def convert_if_webp(file_path: str) -> str:
    if file_path.endswith(".webp"):
        png = Image.open(file_path).convert("RGBA")
        file_path = f"{file_path}.png"
        png.save(file_path, "png")
    return file_path
