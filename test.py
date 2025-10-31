from io import BytesIO
import xlsxwriter

output = BytesIO()

document = xlsxwriter.Workbook(output, options={'in_memory': True})
worksheet = document.add_worksheet()
worksheet.write(0, 0, 'hiiiii')
document.close()
