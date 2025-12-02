## 📚 Databases

---

### 🏦 `financial_entity_freq.dta` (or `.csv`)

Esta base de datos es una sub-muestra de información del master dataset. Esta base  contiene los nombres de las entidades que actúan como assignees (Who is receiving the ownership) en transacciones referentes a “security”.  

**Variables**
- **“ee_name”** — Assignee name (Who is receiving the ownership) 
- **“freq”** — Times it appears as assignee in the master dataset 

> 📝 **Nota para Borja**
>
> - La tarea con esta base de datos es estandarizar los nombres de estas entidades financieras.  
> - Si te fijas, Bank of  America aparece en las primeras filas, pero tiene elementos que hacen que sean diferentes.  
> - La idea es crees un único id y un único nombre que me permita decir que el bank of america que aparecen en la línea 4,5, 6, 11, etc sean el mismo.  
> - Toma como referencia los nombres de la figura 10 del paper “ssrn_2356015”. (No te limites solo a esos)  
> - Para la estandarización, por favor lee el procedimiento descrito en la sección C.2 del mismo paper.  
> - Ademas en el do-file llamado “name_std.do”, hay un código de STATA el cual ayuda a estandarizar ciertas abreviaciones, nombres, etc. Ten ojo con este do-file porque las abreviaciones no son las mismas que encontraras en la base “financial_entity_freq.dta”. Usala como referencia y una ayuda adicional  

---

### 🏢 `Non_financial_entity_freq.dta` (or `.csv`)

Esta base de datos es una sub-muestra de información del master dataset. Esta base  contiene los nombres de las entidades que actúan como assignor (Who is transfering the ownership) en transacciones referentes a “security”.  

**Variables**
- **“or_name”** — Assignor name (Who is transfering the ownership) 
- **“freq”** — Times it appears as assignor in the master dataset 

> 📝 **Nota para Borja**
>
> - La tarea con esta base de datos es la misma que para la base de datos anterior.  
> - Tienes que estandarizar los nombres creando un único id y un único nombre.  