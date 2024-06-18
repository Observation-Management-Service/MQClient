"""Script for benchmarking different message compression options."""

import argparse
import json

TITLE_SPACING = 21
SIZE_SPACING = 8


def compare_size(algo_pkg, data):
    """Compare the size of the data in various compression methods."""

    was_bytes = False
    if isinstance(data, bytes):
        was_bytes = True
        print(f"{'raw bytes':{TITLE_SPACING}} {len(data):{SIZE_SPACING}}")
        import base64

        data = base64.b64encode(data).decode("utf-8")
        print(f"\nthe following use base64 first...\n")

    compressed_size = len(algo_pkg.compress(json.dumps(data).encode()))
    print(
        f"{f'json+encode+{algo_pkg.__name__}':{TITLE_SPACING}} {compressed_size:{SIZE_SPACING}}"
    )

    json_encoded_size = len(json.dumps(data).encode())
    print(
        f"{'only json+encode':{TITLE_SPACING}} {json_encoded_size:{SIZE_SPACING}} (comp. ratio: {compressed_size / json_encoded_size:.3f})"
    )

    import pickle

    if not was_bytes:
        pickle_size = len(pickle.dumps(data))
        print(
            f"{'only pickled':{TITLE_SPACING}} {pickle_size:{SIZE_SPACING}} (comp. ratio: {compressed_size / pickle_size:.3f})"
        )


def compress_decompress(algo_pkg, data):
    if isinstance(data, bytes):
        import base64

        data = base64.b64encode(data).decode("utf-8")
    comp_data = algo_pkg.compress(json.dumps(data).encode())
    out_data = json.loads(algo_pkg.decompress(comp_data))
    # just in case there's a typo, check the data
    assert out_data == data


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--algo", required=True)
    parser.add_argument("--data", required=True)
    parser.add_argument("--only-size", action="store_true")
    args = parser.parse_args()

    # get data

    if args.data == "large_dict":
        data = {
            chr(i): [{chr(k % (i + 1)): k} for k in range(1000)] for i in range(100)
        }
    elif args.data == "medium_string":
        data = "\n".join(
            [
                "The quick brown fox jumps over the lazy dog.",
                "She sells sea shells by the sea shore.",
                "How much wood would a woodchuck chuck if a woodchuck could chuck wood?",
                "Peter Piper picked a peck of pickled peppers.",
                "A journey of a thousand miles begins with a single step.",
                "To be or not to be, that is the question.",
                "All that glitters is not gold.",
                "A picture is worth a thousand words.",
                "Actions speak louder than words.",
                "Better late than never.",
                "Birds of a feather flock together.",
                "Fortune favors the bold.",
                "A watched pot never boils.",
                "Beauty is in the eye of the beholder.",
                "Beggars can't be choosers.",
                "Cleanliness is next to godliness.",
                "Discretion is the better part of valor.",
                "Every cloud has a silver lining.",
                "Good things come to those who wait.",
                "Haste makes waste.",
                "If it ain't broke, don't fix it.",
                "Laughter is the best medicine.",
                "Necessity is the mother of invention.",
                "No man is an island.",
                "Practice makes perfect.",
                "Rome wasn't built in a day.",
                "The early bird catches the worm.",
                "The pen is mightier than the sword.",
            ]
        )
    elif args.data == "skyscan_i3_frame_pkl":
        data = b"\x80\x04\x95\x96\x02\x00\x00\x00\x00\x00\x00\x8c\x10icecube._icetray\x94\x8c\x07I3Frame\x94\x93\x94)R\x94}\x94Bh\x02\x00\x00[i3]\x06\x00\x00\x00\x00\x00P\x05\x00\x00\x00\r\x00\x00\x00I3EventHeader\r\x00\x00\x00I3EventHeader\x86\x00\x00\x00\x00\x01\x02\x00\r\x00\x00\x00I3EventHeader\x01\x03\x00\x00\x00\x00\x01\x00\x01\x00\x00\x00>\x16\x02\x00\x00\x00\x00\x00\x94\x88t\x00\x14\x00\x00\x00\x08\x00\x00\x00#\x00\x00\x00SCAN_nside0001_pixel0008_posvar0000\x01\x00\x02\x00\x00\x00\x03\x00\x00\x00\xe6\x07\x00\x00\xc1\x9d\xde^\xe8,\x18\x02\x04\x00\x00\x00\x05\x00\x00\x00\xe6\x07\x00\x00o\xa0\xe1^\xe8,\x18\x02\x15\x00\x00\x00MillipedeSeedParticle\n\x00\x00\x00I3Particle\x96\x00\x00\x00\x00\x01\x02\x00\n\x00\x00\x00I3Particle\x01\x05\x00\x00\x00\x00\x01\x00\x01\x00\x00\x00\x14\x00\x00\x00N\t\xb7q\xd0\xd0\x14U\x00\x00\x00\x00(\x00\x00\x00\x00\x00\x00\x00\x01\x00\x02\x00\x00\x00\x03\x00\x00\x00\xaeG\xe1z\x14\xdec@H\xe1z\x14\xae\x93u\xc0\n\xd7\xa3p=\x86\x7f\xc0\x01\x00\x04\x00\x00\x00\x05\x00\x00\x00\xa8aF\x06fj\x02@\x88\xe5:\xd1\xc7\x07\x0c@\x00\x00\x00\x00jr&A\x00\x00\x00\x00\x00\x00\xf8\x7f\x00\x00\x00\x00\x00\x00\xf8\x7f\x14\xa3\xac\xb4\xcc/\xd3?\x00\x00\x00\x00\x11\x00\x00\x00SCAN_HealpixNSide\x10\x00\x00\x00I3PODHolder<int>\x1d\x00\x00\x00\x00\x01\x02\x00\x05\x00\x00\x00I3Int\x01\x00\x00\x00\x00\x00\x01\x00\x01\x00\x00\x00\x01\x00\x00\x00\x11\x00\x00\x00SCAN_HealpixPixel\x10\x00\x00\x00I3PODHolder<int>\x1d\x00\x00\x00\x00\x01\x02\x00\x05\x00\x00\x00I3Int\x01\x00\x00\x00\x00\x00\x01\x00\x01\x00\x00\x00\x08\x00\x00\x00\x1b\x00\x00\x00SCAN_PositionVariationIndex\x10\x00\x00\x00I3PODHolder<int>\x1d\x00\x00\x00\x00\x01\x02\x00\x05\x00\x00\x00I3Int\x01\x00\x00\x00\x00\x00\x01\x00\x01\x00\x00\x00\x00\x00\x00\x00\x1aU\x07\xa1\x94\x86\x94b."
    else:
        raise ValueError("invalid --data value")

    # go!

    if args.algo == "bz2":
        import bz2

        if args.only_size:
            compare_size(bz2, data)
        else:
            compress_decompress(bz2, data)

    elif args.algo == "lzma":
        import lzma

        if args.only_size:
            compare_size(lzma, data)
        else:
            compress_decompress(lzma, data)

    elif args.algo == "zstd":
        import zstd  # type: ignore

        if args.only_size:
            compare_size(zstd, data)
        else:
            compress_decompress(zstd, data)

    elif args.algo == "gzip":
        import gzip

        if args.only_size:
            compare_size(gzip, data)
        else:
            compress_decompress(gzip, data)

    elif args.algo == "lz4":
        import lz4.frame  # type: ignore

        if args.only_size:
            compare_size(lz4.frame, data)
        else:
            compress_decompress(lz4.frame, data)

    else:
        raise Exception("invalid compression algorithm specified")


if __name__ == "__main__":
    main()
