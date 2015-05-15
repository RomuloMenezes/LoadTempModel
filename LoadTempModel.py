__author__ = 'rmartins'
from mrjob.job import MRJob
from mrjob.step import MRStep
import numpy


class LoadTempModel(MRJob):
    outliers_range_width = 3  # Values outside the range of mean plus or minus this value times std_dev are excluded

    def steps(self):
        return [MRStep(mapper=self.prepare_data,
                       reducer=self.calc_dispersion),
                MRStep(mapper=self.exclude_outliers,
                       reducer=self.calc_regression),
                MRStep(mapper=self.calc_model_output)]

    def prepare_data(self, _, current_line):

        curr_line_parts = current_line.split(';')
        if curr_line_parts[0] != 'DATA':  # Ignore header
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
            if curr_line_parts[9] == 'O':
                yield (curr_line_parts[7], '%02d' % half_hour_index), (float(curr_line_parts[5]),
                                                                       float(curr_line_parts[6]),
                                                                             curr_line_parts[9])
                        # Key: weekday, half hour index
                        # Value: delta temperature, delta load, input/output
            else:
                if curr_line_parts[5] != '' and curr_line_parts[6] != '' and \
                   curr_line_parts[5] != '#VALOR!' and curr_line_parts[6] != '#VALOR!':  # Exclude empty & invalid data
                    if curr_line_parts[4] != '1':  # Only non holidays are considered
                        if len(curr_line_parts[0]) > 11:
                            yield (curr_line_parts[7], '%02d' % half_hour_index), (float(curr_line_parts[5]),
                                                                                   float(curr_line_parts[6]),
                                                                                         curr_line_parts[9])
                            # Key: weekday, half hour index
                            # Value: delta temperature, delta load, input/output
        # else:
        #     print "Erro!"

    def calc_dispersion(self, key, values):

        input_temperature_values = []
        all_temperature_values = []
        input_load_values = []
        all_load_values = []
        input_output_values = []
        for curr_value in values:
            if curr_value[2] == 'I':
                input_temperature_values.append(curr_value[0])
                input_load_values.append(curr_value[1])
            all_temperature_values.append(curr_value[0])
            all_load_values.append(curr_value[1])
            input_output_values.append(curr_value[2])
        temperature_mean = numpy.mean(input_temperature_values)
        temperature_std_dev = numpy.std(input_temperature_values)
        load_mean = numpy.mean(input_load_values)
        load_std_dev = numpy.std(input_load_values)
        for index in range(len(all_temperature_values)):
            yield key, (all_temperature_values[index], all_load_values[index], temperature_mean, temperature_std_dev,
                        load_mean, load_std_dev, input_output_values[index])
            # Key: weekday, half hour index
            # Value: delta temperature, delta load, temperature mean, temperature std. dev., load mean, load std. dev.,
            #        input/output

    def exclude_outliers(self, key, values):

        temp_value = values[0]
        load_value = values[1]
        temp_mean = values[2]
        temp_std_dev = values[3]
        load_mean = values[4]
        load_std_dev = values[5]
        input_output = values[6]
        temp_lower_limit = temp_mean - self.outliers_range_width * temp_std_dev
        temp_upper_limit = temp_mean + self.outliers_range_width * temp_std_dev
        load_lower_limit = load_mean - self.outliers_range_width * load_std_dev
        load_upper_limit = load_mean + self.outliers_range_width * load_std_dev
        if input_output == 'I':
            if temp_lower_limit <= temp_value <= temp_upper_limit and \
               load_lower_limit <= load_value <= load_upper_limit:
                yield key, [temp_value, load_value, input_output]
        else:
            yield key, [temp_value, load_value, input_output]
            # Key: weekday, half hour index
            # Value: delta temperature, delta load

    def calc_regression(self, key, values):
        all_temperature_values = []
        input_temperature_values = []
        all_load_values = []
        input_load_values = []
        input_output = []
        for curr_value in values:
            curr_temp_value = curr_value[0]
            curr_load_value = curr_value[1]
            curr_input_output = curr_value[2]
            if curr_value[2] == 'I':
                input_temperature_values.append(curr_temp_value)
                input_load_values.append(curr_load_value)
            all_temperature_values.append(curr_temp_value)
            all_load_values.append(curr_load_value)
            input_output.append(curr_input_output)
        input_matrix = numpy.array([input_temperature_values, numpy.ones(len(input_temperature_values))])
        output_matrix = input_load_values
        coefficients = numpy.linalg.lstsq(input_matrix.T, output_matrix)
        for index in range(len(all_temperature_values)):
            yield key, (all_temperature_values[index], all_load_values[index],
                        coefficients[0][0], coefficients[0][1], input_output[index])

    def calc_model_output(self, key, values):
        input_temperature = values[0]
        coefficient_a = values[2]
        coefficient_b = values[3]
        if values[4] == 'O':
            calculated_load = coefficient_a * input_temperature + coefficient_b
            yield key, (input_temperature, calculated_load, coefficient_a, coefficient_b)


if __name__ == '__main__':
    LoadTempModel.run()