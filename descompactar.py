import os

#para salvar progresso: delete a pasta do último ano e recomece a partir dela.

#ctrl-c  pra sair, sem dó

save = 1929

for x in range(save, 2022):
    print("Agora no ano: %i" % x)
    os.makedirs('.\dados\descompactado\{0}'.format(str(x)))
    os.system('tar -xzf .\dados\{0}.tar.gz -C .\dados\descompactado\{1}'.format(str(x),str(x)))