__author__ = 'rmartins'
from mrjob.job import MRJob
from mrjob.step import MRStep
import numpy


class LoadTempModel(MRJob):

    def steps(self):
        return [MRStep(mapper=self.prepare_data,
                       reducer=self.calc_dispersion)]

    def prepare_data(self, _, current_line):

        curr_line_parts = current_line.split(';')
        if curr_line_parts[5] != '' and curr_line_parts[6] != '' and \
           curr_line_parts[5] != '#VALOR!' and curr_line_parts[6] != '#VALOR!':  # Exclude empty and invalid data
            if curr_line_parts[4] == '0':  # Only non holidays are considered (notice that empty values are ignored)
                if len(curr_line_parts[0]) > 11:
                    if curr_line_parts[0][-12:] == '00:00:00.000':
                        half_hour_index = 48
                        if curr_line_parts[7] == '1':
                            curr_line_parts[7] = '7'
                        else:
                            curr_line_parts[7] = str(int(curr_line_parts[7]) - 1)
                    else:
                        if curr_line_parts[0][-9:-7] == '30':
                            half_hour_index = 2 * int(curr_line_parts[0][-12:-10]) + 1
                        else:
                            half_hour_index = 2 * int(curr_line_parts[0][-12:-10])
                    print curr_line_parts[5], curr_line_parts[6]
                    yield (int(curr_line_parts[7]), int(half_hour_index)), (float(curr_line_parts[5]),
                                                                            float(curr_line_parts[6]))

    def calc_dispersion(self, key, values):

        print key, values
        temperature_values = []
        load_values = []
        for curr_value in values:
            temperature_values.append(curr_value[0])
            load_values.append(curr_value[1])
        temperature_mean = numpy.mean(temperature_values)
        temperature_std_dev = numpy.std(temperature_values)
        load_mean = numpy.mean(load_values)
        load_std_dev = numpy.std(load_values)
        for index in range(len(temperature_values)):
            yield key, (temperature_values[index], load_values[index], temperature_mean, temperature_std_dev, load_mean,
                        load_std_dev)


if __name__ == '__main__':
    LoadTempModel.run()