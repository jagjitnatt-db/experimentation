import multiprocessing as mp
import mmap
from datetime import datetime


file_location = "D:\\Development\\Data\BRC\\measurements.txt"

def find_chunks(file):
    chunk_size = 10000000
    f = open(file, 'rb')
    file_size = f.seek(0, 2)
    f.seek(0)
    temp_ranges = list(range(0, file_size, chunk_size))
    final_ranges = []
    for temp_range in temp_ranges[1:]:
        start = f.tell()
        f.seek(temp_range)
        f.readline()
        final_ranges.append((file, start, f.tell()))
    return final_ranges


def process_chunk(metadata):
    file = metadata[0]
    start = metadata[1]
    end = metadata[2]
    length = end - start
    metrics = {}
    f = open(file, 'rb')
    fm = mmap.mmap(f.fileno(), length=0, access=mmap.ACCESS_READ)
    fm.seek(start)
    data = fm.read(length - 1)
    fm.close()
    f.close()
    for record in data.split(b"\n"):
        rec = record.split(b";")
        city, temp_f = rec[0], float(rec[1])
        current_val = metrics.get(city)
        if current_val is not None:
            min_v, max_v, sum_v, cnt_v = current_val
            if temp_f < min_v:
                min_v = temp_f
            if temp_f > max_v:
                max_v = temp_f
            sum_v += temp_f
            cnt_v += 1
            metrics[city] = (min_v, max_v, sum_v, cnt_v)
        else:
            metrics[city] = (temp_f, temp_f, temp_f, 1)
    # [get_metrics(record.split(b";")) for record in data.split(b"\n")]
    return metrics

def process_chunk3(metadata):
    start = metadata[1]
    metrics = {}
    f = open(metadata[0], 'rb')
    fm = mmap.mmap(f.fileno(), length=0, access=mmap.ACCESS_READ)
    fm.seek(metadata[1])
    data = fm.read(metadata[2] - metadata[1] - 1)
    fm.close()
    f.close()
    for record in data.split(b"\n"):
        rec = record.split(b";")
        city, temp_f = rec[0], float(rec[1])
        current_val = metrics.get(city)
        if current_val is not None:
            metrics[city].append(temp_f)
        else:
            metrics[city] = [temp_f]
    
    condensed_metrics = {key:(min(item), max(item), sum(item), len(item)) for key, item in metrics.items()}
    return condensed_metrics
    
def combine_dicts(list_dicts):
    master_dict = {}
    for dict in list_dicts:
        for city, (min_c, max_c, sum_c, cnt_c)  in dict.items():
            current_val = master_dict.get(city)
            if current_val is not None:
                min_v, max_v, sum_v, cnt_v = current_val
                if min_c < min_v:
                    min_v = min_c
                if max_c > max_v:
                    max_v = max_c
                sum_v += sum_c
                cnt_v += cnt_c
                master_dict[city] = (min_v, max_v, sum_v, cnt_v)
            else:
                master_dict[city] = (min_c, max_c, sum_c, cnt_c)
    final_result = {metric.decode("utf-8"): (values[0], values[1], values[2] / values[3]) for metric, values in master_dict.items()}
    return final_result

if __name__ == '__main__':
    start = datetime.now()
    ranges = find_chunks(file_location)
    #print(ranges)

    pool = mp.Pool(processes = 24)
    results = pool.map(process_chunk, ranges)
    # for res in results:
    #     print(res[b"Dunedin"])
    result = combine_dicts(results)
    end = datetime.now()
    print(result)
    print(end - start)
