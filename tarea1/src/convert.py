import abc
import os

class Convert:
    @abc.abstractmethod
    def convert(self,input_filename, output_folder, compression):
        """
        Convierte un archivo de entrada a un formato específico y lo guarda en una carpeta de salida.

        Args:
            input_filename (str): Ruta al archivo de entrada que se desea convertir.
            output_folder (str): Carpeta donde se guardará el archivo convertido.
            compression (str): Tipo de compresión a aplicar al archivo convertido (por ejemplo, 'gzip', 'none').

        Returns:
            None: Este método no devuelve ningún valor, pero realiza la conversión y guarda el archivo en la ubicación especificada.
        """
        return
    
    def get_output_filename(self, input_filename, output_folder, extension, compression):
        """
        Genera el nombre del archivo de salida basado en el nombre del archivo de entrada y la compresión deseada.

        Args:
            input_filename (str): Ruta al archivo de entrada.
            output_folder (str): Carpeta donde se guardará el archivo convertido.
            compression (str): Tipo de compresión a aplicar al archivo convertido.

        Returns:
            str: Ruta completa del archivo de salida.
        """
        output_file = "{}.{}.{}".format(os.path.basename(input_filename), compression, extension)
        return os.path.join(output_folder, output_file)